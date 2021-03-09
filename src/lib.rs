use std::collections::HashMap;

use chrono::{DateTime, FixedOffset};
use ego_tree::NodeRef;
use regex::{Regex, RegexBuilder};
use scraper::{node::Element, Node};
use tendril::{fmt, Atomic, StrTendril, Tendril};
use unicode_segmentation::UnicodeSegmentation;

pub fn parse_document(doc: &str) -> Document {
    let html = scraper::Html::parse_document(doc);
    let root = html.root_element();

    let mut state = ParseState::new();
    state.parse(&root);
    state.flush();

    let mut pipeline = Document {
        text_blocks: state.text_blocks,
        title: state.title,
        time: state.time,
    };

    pipeline.process();

    pipeline
}

struct ParseState {
    flush: bool,
    tag_depth: u64,
    block_tag_depth: i64,
    body_depth: u64,
    anchor_depth: u64,
    last_start_tag: Option<Tag>,
    in_anchor_text: bool,
    label_stack: Vec<Label>,
    text: Tendril<fmt::UTF8, Atomic>,
    title: Option<String>,
    text_blocks: Vec<TextBlock>,
    time: Option<DateTime<FixedOffset>>,
}

const ANCHOR_TEXT_START: &'static str = "$\u{e00a}<";
const ANCHOR_TEXT_END: &'static str = ">\u{e00a}$";
const MAX_LINE_LENGTH: usize = 80;

impl ParseState {
    fn new() -> Self {
        Self {
            flush: false,
            tag_depth: 0,
            block_tag_depth: -1,
            body_depth: 0,
            anchor_depth: 0,
            last_start_tag: None,
            in_anchor_text: false,
            label_stack: vec![],
            text: Tendril::new(),
            title: None,
            text_blocks: vec![],
            time: None,
        }
    }

    fn parse(&mut self, root: &NodeRef<Node>) {
        for node in root.children() {
            match node.value() {
                scraper::Node::Text(t) => {
                    self.text(&t.text);
                }

                scraper::Node::Element(el) => {
                    let tag = Tag::from_str(el.name());

                    self.start_element(node, el, &tag);
                    self.end_element(&tag);
                }

                _ => {} // ignore
            }
        }
    }

    fn text(&mut self, t: &StrTendril) {
        if self.flush {
            self.flush();
            self.flush = false;
        }

        lazy_static::lazy_static! {
            static ref JSON_REGEX: Regex = RegexBuilder::new(r#"(\[.*)?\{.*".*":(\{.*:.*\})?.*\}\]?"#)
                .dot_matches_new_line(true)
                .build()
                .unwrap();
        };

        let t = JSON_REGEX.replace_all(t.trim(), "");

        self.text.push_slice(&t);
        self.text.push_char(' ');

        if self.block_tag_depth == -1 {
            self.block_tag_depth = self.tag_depth as i64;
        }
    }

    fn start_element(&mut self, root: NodeRef<Node>, el: &Element, tag: &Option<Tag>) {
        let action = match tag {
            Some(tag) => {
                let action = tag.action();

                if action.changes_tag_level() {
                    self.tag_depth += 1;
                }

                self.flush = action.should_flush() || self.flush;

                action
            }
            None => {
                self.tag_depth += 1;
                self.flush = true;

                Action::Inline
            }
        };

        match &action {
            Action::Body => {
                self.flush();
                self.body_depth += 1;
            }
            Action::Anchor => {
                self.anchor_depth += 1;
                self.text.push_slice(ANCHOR_TEXT_START);
            }
            Action::BlockTagLabel(labels) => {
                self.label_stack.extend_from_slice(labels);
            }
            Action::Time => {
                self.time = el
                    .attr("datetime")
                    .and_then(|dt| DateTime::parse_from_rfc3339(dt).ok());
            }
            _ => {}
        }

        match action {
            Action::Ingore | Action::IngoreVoid => {}
            _ => {
                self.parse(&root);
            }
        }

        self.last_start_tag = *tag;
    }

    fn end_element(&mut self, tag: &Option<Tag>) {
        let action = match tag {
            Some(tag) => {
                let action = tag.action();

                self.flush = action.should_flush() || self.flush;

                if action.changes_tag_level() {
                    self.tag_depth -= 1;
                }

                action
            }
            None => {
                self.tag_depth -= 1;
                self.flush = true;

                Action::Inline
            }
        };

        match action {
            Action::Body => {
                self.flush();
                self.body_depth -= 1;
            }
            Action::Anchor => {
                self.anchor_depth -= 1;
                if self.anchor_depth == 0 {
                    self.text.push_slice(ANCHOR_TEXT_END);
                }
            }
            _ => {}
        }

        if self.flush {
            self.flush();
        }

        self.label_stack.pop();
    }

    fn flush(&mut self) {
        if self.body_depth == 0 {
            if let Some(Tag::Title) = self.last_start_tag {
                if self.text.len() > 0 {
                    self.title = self.text.trim().to_string().into();
                }
            }

            self.text.clear();
            return;
        }

        if self.text.len() == 0 || self.text.len() == 1 {
            self.text.clear();
            return;
        }

        {
            let mut num_words = 0;
            let mut num_linked_words = 0;
            let mut num_wrapped_lines = 0;
            let mut num_tokens = 0;
            let mut num_words_current_line = 0;
            let mut current_line_length = 0;

            lazy_static::lazy_static! {
                static ref RE_SPACE: Regex = RegexBuilder::new(r#"[ ]+"#)
                    .dot_matches_new_line(true)
                    .build()
                    .unwrap();
                static ref RE_WORD: Regex = RegexBuilder::new(r#"[\p{L}\p{Nd}\p{Nl}\p{No}]"#)
                    .dot_matches_new_line(true)
                    .build()
                    .unwrap();
            };
            let text = Self::tokenize(&self.text);

            for word in RE_SPACE.split(&text) {
                match word {
                    ANCHOR_TEXT_START => {
                        self.in_anchor_text = true;
                    }
                    ANCHOR_TEXT_END => {
                        self.in_anchor_text = false;
                    }
                    word if RE_WORD.is_match(word) => {
                        num_tokens += 1;
                        num_words += 1;
                        num_words_current_line += 1;

                        if self.in_anchor_text {
                            num_linked_words += 1;
                        }

                        current_line_length += word.len() + 1;

                        if current_line_length > MAX_LINE_LENGTH {
                            num_wrapped_lines += 1;
                            current_line_length = word.len();
                            num_words_current_line = 1;
                        }
                    }
                    "" => {}
                    _ => {
                        num_tokens += 1;
                    }
                }
            }

            if num_tokens == 0 {
                return;
            }

            let num_words_in_wrapped_lines = if num_wrapped_lines == 0 {
                num_wrapped_lines = 1;

                num_words
            } else {
                num_words - num_words_current_line
            };

            let text = self
                .text
                .trim()
                .replace(ANCHOR_TEXT_START, "")
                .replace(ANCHOR_TEXT_END, "");

            if text.len() > 0 {
                let mut text_block = TextBlock {
                    text: text.into(),
                    num_words,
                    num_linked_words,
                    num_words_in_wrapped_lines,
                    num_wrapped_lines,
                    offset_block_start: self.text_blocks.len(),
                    offset_block_end: self.text_blocks.len(),
                    tag_level: self.block_tag_depth as usize,
                    is_content: true,
                    label_map: HashMap::new(),
                };

                text_block.add_labels(&self.label_stack);
                self.label_stack.clear();

                self.text_blocks.push(text_block);
            }

            self.text.clear();
            self.block_tag_depth = -1;
        }
    }

    fn tokenize(s: &str) -> String {
        lazy_static::lazy_static! {
            static ref RE_WORD_BOUNDARY: Regex = RegexBuilder::new(r#"[\pN\d_]+"#)
                .dot_matches_new_line(true)
                .build()
                .unwrap();
            static ref RE_NOT_WORD_BOUNDARY: Regex = RegexBuilder::new(r#"[\u{2063}]*([\\"'\\.,\\!\\@\\-\\:\\;\\$\\?\\(\\)/])[\u{2063}]*"#)
                .dot_matches_new_line(true)
                .build()
                .unwrap();
            static ref RE_INVISIBLE_SEP: Regex = RegexBuilder::new("[\u{2063}]+")
                .dot_matches_new_line(true)
                .build()
                .unwrap();
        };

        let s = RE_WORD_BOUNDARY.replace_all(s.trim(), |caps: &regex::Captures| {
            format!("\u{2063}{}\u{2063}", &caps[0])
        });
        let s = RE_NOT_WORD_BOUNDARY.replace_all(&s, "$1");
        let s = RE_INVISIBLE_SEP.replace_all(&s, " ");

        s.trim().to_owned()
    }
}

#[derive(Debug, Copy, Clone)]
enum Tag {
    Applet,
    Figcaption,
    Figure,
    Noscript,
    Object,
    Option,
    Script,
    Style,
    A,
    Body,
    Abbr,
    B,
    Code,
    Em,
    Font,
    I,
    Span,
    Strike,
    Strong,
    Sub,
    Sup,
    Tt,
    U,
    Var,
    Li,
    H1,
    H2,
    H3,
    Area,
    Base,
    Br,
    Col,
    Embed,
    Hr,
    Img,
    Input,
    Link,
    Menuitem,
    Meta,
    Param,
    Source,
    Track,
    Wbr,
    Time,
    Title,
}

impl Tag {
    fn from_str(s: &str) -> Option<Self> {
        match s {
            "applet" => Some(Self::Applet),
            "figcaption" => Some(Self::Figcaption),
            "figure" => Some(Self::Figure),
            "noscript" => Some(Self::Noscript),
            "object" => Some(Self::Object),
            "option" => Some(Self::Option),
            "script" => Some(Self::Script),
            "style" => Some(Self::Style),
            "a" => Some(Self::A),
            "body" => Some(Self::Body),
            "abbr" => Some(Self::Abbr),
            "b" => Some(Self::B),
            "code" => Some(Self::Code),
            "em" => Some(Self::Em),
            "font" => Some(Self::Font),
            "i" => Some(Self::I),
            "span" => Some(Self::Span),
            "strike" => Some(Self::Strike),
            "strong" => Some(Self::Strong),
            "sub" => Some(Self::Sub),
            "sup" => Some(Self::Sup),
            "tt" => Some(Self::Tt),
            "u" => Some(Self::U),
            "var" => Some(Self::Var),
            "li" => Some(Self::Li),
            "h1" => Some(Self::H1),
            "h2" => Some(Self::H2),
            "h3" => Some(Self::H3),
            "area" => Some(Self::Area),
            "base" => Some(Self::Base),
            "br" => Some(Self::Br),
            "col" => Some(Self::Col),
            "embed" => Some(Self::Embed),
            "hr" => Some(Self::Hr),
            "img" => Some(Self::Img),
            "input" => Some(Self::Input),
            "link" => Some(Self::Link),
            "menuitem" => Some(Self::Menuitem),
            "meta" => Some(Self::Meta),
            "param" => Some(Self::Param),
            "source" => Some(Self::Source),
            "track" => Some(Self::Track),
            "wbr" => Some(Self::Wbr),
            "time" => Some(Self::Time),
            "title" => Some(Self::Title),

            _ => None,
        }
    }

    fn action(&self) -> Action {
        match self {
            Tag::Applet
            | Tag::Figcaption
            | Tag::Figure
            | Tag::Noscript
            | Tag::Object
            | Tag::Option
            | Tag::Script
            | Tag::Style => Action::Ingore,

            Tag::A => Action::Anchor,

            Tag::Body => Action::Body,

            Tag::Abbr
            | Tag::B
            | Tag::Code
            | Tag::Em
            | Tag::Font
            | Tag::I
            | Tag::Span
            | Tag::Strike
            | Tag::Strong
            | Tag::Sub
            | Tag::Sup
            | Tag::Tt
            | Tag::U
            | Tag::Var => Action::Inline,

            Tag::Li => Action::BlockTagLabel(vec![Label::List]),
            Tag::H1 => Action::BlockTagLabel(vec![Label::Heading, Label::Heading1]),
            Tag::H2 => Action::BlockTagLabel(vec![Label::Heading, Label::Heading2]),
            Tag::H3 => Action::BlockTagLabel(vec![Label::Heading, Label::Heading3]),

            Tag::Area
            | Tag::Base
            | Tag::Br
            | Tag::Col
            | Tag::Embed
            | Tag::Hr
            | Tag::Img
            | Tag::Input
            | Tag::Link
            | Tag::Menuitem
            | Tag::Meta
            | Tag::Param
            | Tag::Source
            | Tag::Track
            | Tag::Wbr => Action::IngoreVoid,

            Tag::Time => Action::Time,
            Tag::Title => Action::Title,
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
enum Label {
    EndOfText,
    MightBeContent,
    VeryLikelyContent,
    Title,
    List,
    Heading,
    Heading1,
    Heading2,
    Heading3,
}

enum Action {
    Ingore,
    Anchor,
    Body,
    Inline,
    BlockTagLabel(Vec<Label>),
    IngoreVoid,
    Time,
    Title,
}

impl Action {
    fn should_flush(&self) -> bool {
        match self {
            Action::Ingore => true,
            Action::Anchor => false,
            Action::Body => false,
            Action::Inline => false,
            Action::BlockTagLabel(_) => true,
            Action::IngoreVoid => false,
            Action::Time => true,
            Action::Title => true,
        }
    }

    fn changes_tag_level(&self) -> bool {
        match self {
            Action::Ingore => true,
            Action::Anchor => true,
            Action::Body => true,
            Action::Inline => false,
            Action::BlockTagLabel(_) => true,
            Action::IngoreVoid => false,
            Action::Time => true,
            Action::Title => false,
        }
    }
}

#[derive(Debug)]
struct TextBlock {
    text: Tendril<fmt::UTF8, Atomic>,
    num_words: usize,
    num_linked_words: usize,
    num_words_in_wrapped_lines: usize,
    num_wrapped_lines: usize,
    offset_block_start: usize,
    offset_block_end: usize,
    tag_level: usize,
    is_content: bool,
    label_map: HashMap<Label, usize>,
}

impl TextBlock {
    fn add_labels(&mut self, labels: &[Label]) {
        for label in labels {
            let lbl = self.label_map.entry(*label).or_insert(0);
            *lbl += 1;
        }
    }

    fn link_density(&self) -> f64 {
        if self.num_words == 0 {
            return 0.0;
        }

        return self.num_linked_words as f64 / self.num_words as f64;
    }

    fn text_density(&self) -> f64 {
        self.num_words_in_wrapped_lines as f64 / self.num_wrapped_lines as f64
    }

    fn empty_start() -> Self {
        Self {
            text: Tendril::new(),
            num_words: 0,
            num_linked_words: 0,
            num_words_in_wrapped_lines: 0,
            num_wrapped_lines: 0,
            offset_block_start: std::usize::MIN,
            offset_block_end: std::usize::MIN,
            tag_level: 0,
            is_content: false,
            label_map: HashMap::new(),
        }
    }

    fn empty_end() -> Self {
        Self {
            text: Tendril::new(),
            num_words: 0,
            num_linked_words: 0,
            num_words_in_wrapped_lines: 0,
            num_wrapped_lines: 0,
            offset_block_start: std::usize::MAX,
            offset_block_end: std::usize::MAX,
            tag_level: 0,
            is_content: false,
            label_map: HashMap::new(),
        }
    }

    fn merge(&mut self, other: &Self) {
        self.text.push_char('\n');
        self.text.push_tendril(&other.text);

        self.offset_block_start = std::cmp::min(self.offset_block_start, other.offset_block_start);
        self.offset_block_end = std::cmp::max(self.offset_block_end, other.offset_block_end);

        self.num_words += other.num_words;
        self.num_linked_words += other.num_linked_words;
        self.num_words_in_wrapped_lines += other.num_words_in_wrapped_lines;
        self.num_wrapped_lines += other.num_wrapped_lines;

        self.is_content |= other.is_content;
        self.tag_level = std::cmp::min(self.tag_level, other.tag_level);

        for (k, v) in other.label_map.iter() {
            let count = self.label_map.entry(*k).or_insert(0);
            *count += v;
        }
    }
}

pub struct Document {
    text_blocks: Vec<TextBlock>,
    pub title: Option<String>,
    pub time: Option<DateTime<FixedOffset>>,
}

impl Document {
    fn process(&mut self) -> bool {
        let mut has_changed = self.terminating_blocks();
        has_changed |= self.document_title_match();
        has_changed |= self.num_words_rules_classifier();
        has_changed |= self.ignore_block_after_content();
        has_changed |= self.trailing_headline_to_boilerplate();
        has_changed |= self.block_proximity_fusion(1, false, false);
        has_changed |= self.boilerplate_block();
        has_changed |= self.block_proximity_fusion(1, true, true);
        has_changed |= self.keep_largest_blocks();
        has_changed |= self.expand_title_to_content();
        has_changed |= self.large_block_same_tag_level_to_content();
        has_changed |= self.list_at_end();

        has_changed
    }

    fn terminating_blocks(&mut self) -> bool {
        let mut has_changed = false;

        for tb in self.text_blocks.iter_mut() {
            if tb.num_words < 15 {
                if tb.text.len() >= 8 {
                    let s = tb.text.to_lowercase();

                    if s.starts_with("comments")
                        || s.starts_with("© reuters")
                        || Self::starts_with_number(
                            &tb.text,
                            &[
                                " comments",
                                " users responded in",
                                " комментария",
                                " комментариев",
                            ],
                        )
                        || s.starts_with("please rate this")
                        || s.starts_with("post a comment")
                        || s.contains("what you think...")
                        || s.contains("add your comment")
                        || s.contains("add comment")
                        || s.contains("reader views")
                        || s.contains("have your say")
                        || s.contains("rätta artikeln")
                        || s.contains("оставьте комментарий")
                        || s.contains("расскажите нам, что вы думаете")
                    {
                        tb.add_labels(&[Label::EndOfText]);
                        has_changed = true;
                    }
                } else if tb.link_density() == 1.0 {
                    if tb.text.as_ref() == "Comment" || tb.text.as_ref() == "Комментарии"
                    {
                        tb.add_labels(&[Label::EndOfText]);
                    }
                }
            }
        }

        has_changed
    }

    fn starts_with_number(text: &str, prefixes: &[&str]) -> bool {
        let has_numbers = text
            .grapheme_indices(true)
            .find(|(_i, g)| g.chars().all(|c| c.is_digit(10)));

        match has_numbers {
            Some((i, _)) => {
                for p in prefixes {
                    if text[i..].starts_with(p) {
                        return true;
                    }
                }

                false
            }
            None => false,
        }
    }

    fn document_title_match(&mut self) -> bool {
        let title = match &self.title {
            Some(t) if t.is_empty() => {
                return false;
            }
            Some(t) => t,
            None => {
                return false;
            }
        };

        let title = title.replace("\u{00a0}", " ");
        let title = title.replace("'", "");
        let title = title.trim().to_lowercase();

        if title.is_empty() {
            return false;
        }

        use std::borrow::Cow;

        let mut potential_titles: HashMap<Cow<str>, bool> = HashMap::new();
        potential_titles.insert(title.as_str().into(), true);

        lazy_static::lazy_static! {
            static ref REGEXES_1: [Regex; 6] = [
                Regex::new("[ ]*[\\|»|-][ ]*").unwrap(),
                Regex::new("[ ]*[\\|»|:][ ]*").unwrap(),
                Regex::new("[ ]*[\\|»|:\\(\\)][ ]*").unwrap(),
                Regex::new("[ ]*[\\|»|:\\(\\)\\-][ ]*").unwrap(),
                Regex::new("[ ]*[\\|»|,|:\\(\\)\\-][ ]*").unwrap(),
                Regex::new("[ ]*[\\|»|,|:\\(\\)\\-\u{00a0}][ ]*").unwrap(),
            ];
        }

        for r in REGEXES_1.iter() {
            let potential_title = Self::get_longest_part(&title, &r);
            if !potential_title.is_empty() {
                potential_titles.insert(potential_title.into(), true);
            }
        }

        lazy_static::lazy_static! {
            static ref REGEXES_2: [Regex; 2] = [
                Regex::new("[ ]+[\\|][ ]+").unwrap(),
                Regex::new("[ ]+[\\-][ ]+").unwrap(),
            ];
        }

        for r in REGEXES_2.iter() {
            let parts_count = r.split(&title).count();

            if parts_count == 1 {
                continue;
            }

            let parts = r.split(&title);

            for part in parts {
                if part.contains(".com") {
                    continue;
                }

                let num_words = part.unicode_words().count();
                if num_words >= 4 {
                    potential_titles.insert(part.into(), true);
                }
            }
        }

        lazy_static::lazy_static! {
            static ref REGEXES_3: [Regex; 2] = [
                Regex::new(" - [^\\-]+$").unwrap(),
                Regex::new("^[^\\-]+ - ").unwrap(),
            ];
        };

        for r in REGEXES_3.iter() {
            let potential_title = r.replacen(&title, 1, "");
            potential_titles.insert(potential_title, true);
        }

        let mut has_changed = false;

        lazy_static::lazy_static! {
            static ref REMOVE_RE: Regex = Regex::new(r"[?!.-:]+").unwrap();
        }

        for tb in self.text_blocks.iter_mut() {
            let text = tb.text.replace("\u{00a0}", " ");
            let text = text.replace("'", "");
            let text = text.trim().to_lowercase();

            if potential_titles.contains_key(text.as_str().into()) {
                tb.add_labels(&[Label::Title]);
                has_changed = true;
                break;
            }

            let text = REMOVE_RE.replace(&text, "");
            let text = text.trim();

            if potential_titles.contains_key(text) {
                tb.add_labels(&[Label::Title]);
                has_changed = true;
                break;
            }
        }

        has_changed
    }

    fn get_longest_part<'s>(title: &'s str, r: &regex::Regex) -> &'s str {
        let parts = r.split(title);

        let mut longest_num_words = 0;
        let mut longest_part = "";
        let mut parts_count = 0;

        for part in parts {
            if part.contains(".com") {
                continue;
            }

            let num_words = part.unicode_words().count();
            if num_words > longest_num_words || part.len() > longest_part.len() {
                longest_num_words = num_words;
                longest_part = part;
            }

            parts_count += 1;
        }

        if longest_part.is_empty() || parts_count == 1 {
            return "";
        }

        longest_part.trim()
    }

    fn num_words_rules_classifier(&mut self) -> bool {
        if self.text_blocks.is_empty() {
            return false;
        }

        let mut has_changed = false;

        let empty_start = TextBlock::empty_start();
        let empty_end = TextBlock::empty_end();

        for i in 0..self.text_blocks.len() {
            let window = i
                .checked_sub(1)
                .and_then(|i_1| self.text_blocks.get_mut(i_1..=i + 1));

            let (prev, cur, next): (&TextBlock, &mut TextBlock, &TextBlock) = match window {
                Some([prev, cur, next]) => (&*prev, cur, &*next),
                Some(w) => {
                    debug_assert_eq!(w.len(), 3);
                    continue;
                }
                None => match self.text_blocks.get_mut(i..=i + 1) {
                    Some([cur, next]) => (&empty_start, cur, &*next),
                    Some(w) => {
                        debug_assert_eq!(w.len(), 2);
                        continue;
                    }
                    None => match self.text_blocks.get_mut(i - 1..=i) {
                        Some([prev, cur]) => (&*prev, cur, &empty_end),
                        Some(w) => {
                            debug_assert_eq!(w.len(), 2);
                            continue;
                        }
                        None => match self.text_blocks.get_mut(i) {
                            Some(cur) => (&empty_start, cur, &empty_start),
                            None => {
                                continue;
                            }
                        },
                    },
                },
            };

            let is_content = Self::classify_is_content(prev, cur, next);
            cur.is_content = is_content;
            has_changed |= is_content;
        }

        has_changed
    }

    fn classify_is_content(prev: &TextBlock, cur: &TextBlock, next: &TextBlock) -> bool {
        let mut is_content = false;

        if cur.link_density() <= 0.333333 {
            if prev.link_density() <= 0.555556 {
                if cur.num_words <= 16 {
                    if next.num_words <= 15 {
                        if prev.num_words <= 4 {
                            is_content = false;
                        } else {
                            is_content = true;
                        }
                    } else {
                        is_content = true;
                    }
                } else {
                    is_content = true;
                }
            } else {
                if cur.num_words <= 40 {
                    if next.num_words <= 17 {
                        is_content = false;
                    } else {
                        is_content = true;
                    }
                }
            }
        } else {
            is_content = false;
        }

        is_content
    }

    fn ignore_block_after_content(&mut self) -> bool {
        let mut has_changed = false;
        let mut num_words = 0;
        let mut found_end_of_text = false;

        for tb in self.text_blocks.iter_mut() {
            let end_of_text = tb.label_map.contains_key(&Label::EndOfText);

            if tb.is_content {
                num_words += Self::get_num_full_text_words(tb);
            }

            if end_of_text && num_words >= 60 {
                found_end_of_text = true;
            }

            if found_end_of_text {
                has_changed = true;
                tb.is_content = false;
            }
        }

        has_changed
    }

    fn get_num_full_text_words(tb: &TextBlock) -> usize {
        let min_text_density = 9.0;

        if tb.text_density() >= min_text_density {
            tb.num_words
        } else {
            0
        }
    }

    fn trailing_headline_to_boilerplate(&mut self) -> bool {
        let mut has_changed = false;

        for tb in self.text_blocks.iter_mut().rev() {
            if tb.is_content {
                if tb.label_map.contains_key(&Label::Heading) {
                    tb.is_content = false;
                    has_changed = true;
                } else {
                    break;
                }
            }
        }

        has_changed
    }

    fn block_proximity_fusion(
        &mut self,
        max_block_distance: usize,
        content_only: bool,
        same_tag_level_only: bool,
    ) -> bool {
        if self.text_blocks.len() < 2 {
            return false;
        }

        let mut has_changed = false;
        let mut prev_block = 0;
        let mut start_block = 0;

        if content_only {
            for (i, tb) in self.text_blocks.iter().enumerate() {
                start_block += 1;

                if tb.is_content {
                    prev_block = i;
                    break;
                }
            }

            if prev_block == 0 {
                return false;
            }
        } else {
            prev_block = 0;
            start_block = 1;
        }

        let mut i = start_block;
        let mut to_remove = None;

        loop {
            if let Some(i) = to_remove {
                self.text_blocks.remove(i);
                to_remove = None;
            }

            match self.text_blocks.get_mut(prev_block..=i) {
                Some([prev, cur]) => {
                    if cur.is_content == false {
                        prev_block = i;
                    } else {
                        let diff_blocks = cur.offset_block_end - cur.offset_block_start + 1;
                        if diff_blocks <= max_block_distance {
                            let mut merge = true;

                            if content_only {
                                if !prev.is_content || !cur.is_content {
                                    merge = false;
                                }
                            }

                            if merge && same_tag_level_only && prev.tag_level != cur.tag_level {
                                merge = false;
                            }

                            if merge {
                                prev.merge(cur);
                                to_remove = Some(i);
                                i -= 1;

                                has_changed = true;
                            } else {
                                prev_block += 1;
                            }
                        } else {
                            prev_block += 1;
                        }
                    }
                }
                Some(w) => {
                    debug_assert_eq!(w.len(), 2);
                    break;
                }
                None => {
                    break;
                }
            }

            i += 1;
        }

        has_changed
    }

    fn boilerplate_block(&mut self) -> bool {
        let mut has_changed = false;

        let mut i = 0;

        loop {
            let remove = match self.text_blocks.get(i) {
                Some(tb) => !tb.is_content && !tb.label_map.contains_key(&Label::Title),
                None => {
                    break;
                }
            };

            if remove {
                self.text_blocks.remove(i);
                if i > 0 {
                    i -= 1;
                }
                has_changed = true;
            }

            i += 1;
        }

        has_changed
    }

    fn keep_largest_blocks(&mut self) -> bool {
        let expand_to_same_level_text = true;
        let min_words = 150;

        if self.text_blocks.len() < 2 {
            return false;
        }

        let mut max_num_words = 0;
        let mut largest_block = 0;
        let mut level = 0;
        let mut j = 0;
        let mut n = -1;

        for (i, tb) in self.text_blocks.iter().enumerate() {
            if tb.is_content {
                if tb.num_words > max_num_words {
                    largest_block = i;
                    max_num_words = tb.num_words;
                    n = j;

                    if expand_to_same_level_text {
                        level = tb.tag_level;
                    }
                }
            }

            j += 1;
        }

        for (i, tb) in self.text_blocks.iter_mut().enumerate() {
            if i == largest_block {
                tb.is_content = true;
                tb.add_labels(&[Label::VeryLikelyContent]);
            } else {
                tb.is_content = Self::is_largest_block(max_num_words, tb);
                tb.add_labels(&[Label::MightBeContent]);
            }
        }

        if expand_to_same_level_text && n != -1 {
            for tb in self.text_blocks.iter_mut().rev() {
                if tb.tag_level < level {
                    break;
                } else if tb.tag_level == level {
                    if tb.num_words >= min_words {
                        tb.is_content = true;
                    }
                }
            }

            for tb in self.text_blocks.iter_mut() {
                if tb.tag_level < level {
                    break;
                } else if tb.tag_level == level {
                    if tb.num_words >= min_words {
                        tb.is_content = true;
                    }
                }
            }
        }

        true
    }

    fn is_largest_block(max_num_words: usize, tb: &TextBlock) -> bool {
        let min_word_percent = match max_num_words {
            n if n >= 1000 => 0.25,
            n if n >= 500 => 0.6,
            _ => {
                return tb.is_content && tb.num_words == max_num_words;
            }
        };

        tb.is_content && tb.num_words >= (min_word_percent * max_num_words as f64).trunc() as usize
    }

    fn expand_title_to_content(&mut self) -> bool {
        let mut j = 0;
        let mut title = -1;
        let mut content_start = -1;

        for tb in self.text_blocks.iter() {
            if content_start == -1 && tb.label_map.contains_key(&Label::Title) {
                title = j;
                content_start = -1;
            }

            if content_start == -1 && tb.is_content {
                content_start = j;
            }

            j += 1;
        }

        if content_start <= title || title == -1 {
            return false;
        }

        let mut has_changed = false;

        match self
            .text_blocks
            .get_mut(title as usize..content_start as usize)
        {
            Some(tbs) => {
                for tb in tbs {
                    if tb.label_map.contains_key(&Label::MightBeContent) {
                        has_changed |= !tb.is_content;
                        tb.is_content = true;
                    }
                }
            }
            None => {}
        }

        has_changed
    }

    fn large_block_same_tag_level_to_content(&mut self) -> bool {
        let mut has_changed = false;
        let mut tag_level = None;

        for tb in self.text_blocks.iter() {
            if tb.is_content && tb.label_map.contains_key(&Label::VeryLikelyContent) {
                tag_level = Some(tb.tag_level);
                break;
            }
        }

        let tag_level = match tag_level {
            Some(tl) => tl,
            None => {
                return false;
            }
        };

        for tb in self.text_blocks.iter_mut() {
            if !tb.is_content {
                if tb.num_words >= 100 && tb.tag_level == tag_level {
                    tb.is_content = true;
                    has_changed = true;
                }
            }
        }

        has_changed
    }

    fn list_at_end(&mut self) -> bool {
        let mut has_changed = false;
        let mut tag_level = std::usize::MAX;

        for tb in self.text_blocks.iter_mut() {
            if tb.is_content && tb.label_map.contains_key(&Label::VeryLikelyContent) {
                tag_level = tb.tag_level;
            } else {
                if tb.tag_level > tag_level
                    && tb.label_map.contains_key(&Label::MightBeContent)
                    && tb.label_map.contains_key(&Label::List)
                    && tb.link_density() == 0.0
                {
                    tb.is_content = true;
                    has_changed = true;
                } else {
                    tag_level = std::usize::MAX;
                }
            }
        }

        has_changed
    }

    fn text(&self, include_content: bool, include_non_content: bool) -> Tendril<fmt::UTF8, Atomic> {
        let mut text: Tendril<fmt::UTF8, Atomic> = Tendril::new();

        for tb in self.text_blocks.iter() {
            if tb.is_content {
                if !include_content {
                    continue;
                }
            } else {
                if !include_non_content {
                    continue;
                }
            }

            text.push_tendril(&tb.text);
        }

        return text;
    }

    pub fn content(&self) -> Tendril<fmt::UTF8, Atomic> {
        self.text(true, false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() {
        for i in 0..=12 {
            let html_file = format!("test-data/{}.html", i);
            let b64_file = format!("test-data/{}.base64", i);

            let html = std::fs::read(html_file).unwrap();
            let s = String::from_utf8(html).unwrap();

            let doc = parse_document(&s);
            let content = doc.content();
            println!("{}", content.to_string());
            let b64 = base64::encode(content.as_bytes());

            let base64 = std::fs::read(b64_file).unwrap();
            let expected_base64 = String::from_utf8(base64).unwrap();

            println!("{:?}", doc.title);
            println!("{:?}", doc.time);

            assert_eq!(b64, expected_base64);
        }
    }
}
