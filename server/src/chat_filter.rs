use finl_unicode::categories::{CharacterCategories, MinorCategory};
use rustrict::{Censor, Trie, Type};
use std::ops::Range;
use std::sync::LazyLock;
use unicode_normalization::UnicodeNormalization;
use unicode_segmentation::UnicodeSegmentation;

const CENSOR_MARKER: char = '\u{e000}';

/// These are well-known substring false positives rather than exemptions for
/// abusive words. Rustrict only applies `Type::NONE` to an exact, unobfuscated
/// match, so the underlying bad-word detection remains active for evasions.
static CHAT_FILTER_TRIE: LazyLock<Trie> = LazyLock::new(|| {
    let mut trie = Trie::default();
    for word in [
        "scunthorpe",
        "shiitake",
        "shitake",
        "middlesex",
        "middlesex's",
        "gaylord",
        "dickinson",
        "Niger",
    ] {
        trie.set(word, Type::NONE);
    }
    trie
});

#[derive(Clone, Copy)]
struct NormalizedCharacter {
    character: char,
    original_start: usize,
    original_end: usize,
}

#[derive(Debug, Clone)]
struct Token {
    start: usize,
    end: usize,
    text: String,
    clause_boundary_before: bool,
    tight_clause_boundary_before: bool,
    direct_label_boundary_before: bool,
}

/// Replaces inappropriate chat content using rustrict's public profanity and
/// offensive-language trie. The detector normalizes common substitutions,
/// confusable Unicode, repeated characters, accents, and inserted separators.
///
/// Clean text is returned verbatim so moderation does not remove legitimate
/// accents. Bidirectional formatting controls are still removed because they
/// can make the rendered text differ from the text the detector analyzed.
pub(crate) fn filter_chat_message(message: &str) -> String {
    let mut censor = Censor::from_str(message);
    let filtered = censor
        .with_trie(&CHAT_FILTER_TRIE)
        .with_censor_threshold(Type::INAPPROPRIATE)
        .with_censor_first_character_threshold(Type::INAPPROPRIATE)
        .with_ignore_false_positives(false)
        .with_censor_replacement(CENSOR_MARKER)
        .censor();

    // Contextual patterns must run even when the public trie considers every
    // individual word clean (for example, "kill all Christians").
    let filtered = mask_detected_content(message, filtered);

    filtered
        .chars()
        .filter(|character| !is_bidi_control(*character))
        .collect()
}

fn mask_detected_content(original: &str, filtered: String) -> String {
    let original_characters: Vec<char> = original.chars().collect();
    let normalized_characters = normalize_with_mapping(original);
    let filtered_characters: Vec<char> = filtered.chars().collect();
    let normalized_text: Vec<char> = normalized_characters
        .iter()
        .map(|character| character.character)
        .collect();

    if normalized_characters.len() != filtered_characters.len()
        || normalized_characters
            .iter()
            .zip(&filtered_characters)
            .any(|(source, filtered)| *filtered != CENSOR_MARKER && *filtered != source.character)
    {
        return mask_normalized_content(
            rustrict_normalized_characters(original),
            filtered_characters,
        );
    }

    let mut masked = original_characters.clone();
    for range in content_mask_ranges(&normalized_text, &filtered_characters) {
        let original_start = normalized_characters[range.start].original_start;
        let original_end = normalized_characters[range.end - 1].original_end;
        for character in &mut masked[original_start..original_end] {
            *character = '*';
        }
    }

    masked.into_iter().collect()
}

fn normalize_with_mapping(message: &str) -> Vec<NormalizedCharacter> {
    let mut decomposed = Vec::with_capacity(message.chars().count());

    for (original_start, character) in message.chars().enumerate() {
        let original_end = original_start + 1;
        let mapped_start = if decomposed.is_empty() {
            0
        } else {
            original_start
        };
        let mut kept_any = false;

        for decomposed_character in std::iter::once(character).nfd() {
            if rustrict_keeps_normalized_character(&decomposed_character) {
                decomposed.push(NormalizedCharacter {
                    character: decomposed_character,
                    original_start: mapped_start,
                    original_end,
                });
                kept_any = true;
            }
        }

        if !kept_any && let Some(previous) = decomposed.last_mut() {
            previous.original_end = original_end;
        }
    }

    let decomposed_text: String = decomposed
        .iter()
        .map(|character| character.character)
        .collect();
    let mut normalized = Vec::with_capacity(decomposed.len());
    let mut decomposed_index = 0;

    for grapheme in UnicodeSegmentation::graphemes(decomposed_text.as_str(), true) {
        let grapheme_len = grapheme.chars().count();
        let grapheme_end = decomposed_index + grapheme_len;
        let original_start = decomposed[decomposed_index].original_start;
        let original_end = decomposed[grapheme_end - 1].original_end;

        normalized.extend(grapheme.chars().nfc().map(|character| NormalizedCharacter {
            character,
            original_start,
            original_end,
        }));
        decomposed_index = grapheme_end;
    }

    normalized
}

fn rustrict_keeps_normalized_character(character: &char) -> bool {
    let category = character.get_minor_category();
    let preserve_japanese_mark = matches!(*character, '\u{3099}' | '\u{309a}');
    let removed_category = matches!(
        category,
        MinorCategory::Cn | MinorCategory::Co | MinorCategory::Mn
    ) && !preserve_japanese_mark;

    !removed_category && !is_rustrict_banned_character(*character)
}

fn is_rustrict_banned_character(character: char) -> bool {
    matches!(
        character,
        '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}' | '\u{fc60}'
    )
}

fn rustrict_normalized_characters(message: &str) -> Vec<char> {
    let mut normalizer = Censor::from_str(message);
    normalizer
        .with_censor_threshold(Type::NONE)
        .censor()
        .chars()
        .collect()
}

fn content_mask_ranges(source: &[char], filtered: &[char]) -> Vec<Range<usize>> {
    if source.len() != filtered.len() {
        return Vec::new();
    }

    let tokens = tokenize(source);
    let identity_spans = identity_token_spans(&tokens);
    let hate_ranges = contextual_hate_ranges(&tokens, &identity_spans);
    let mut ranges = hate_ranges.clone();
    let mut index = 0;

    while index < filtered.len() {
        if filtered[index] != CENSOR_MARKER {
            index += 1;
            continue;
        }

        let marker_start = index;
        while index < filtered.len() && filtered[index] == CENSOR_MARKER {
            index += 1;
        }
        let marker_end = index;
        let mask_start =
            corrected_marker_start(source, marker_start, marker_end, &tokens, &identity_spans);
        let marker_range = mask_start..marker_end;

        if is_neutral_identity_range(&marker_range, &tokens, &identity_spans)
            && !hate_ranges
                .iter()
                .any(|hate_range| ranges_overlap(&marker_range, hate_range))
        {
            continue;
        }
        ranges.push(marker_range);
    }

    ranges
}

fn corrected_marker_start(
    source: &[char],
    marker_start: usize,
    marker_end: usize,
    tokens: &[Token],
    identity_spans: &[Range<usize>],
) -> usize {
    if match_starts_inside_word(source, marker_start)
        && let Some(suffix_start) = direct_inappropriate_suffix(source, marker_start, marker_end)
    {
        return suffix_start;
    }

    neutral_identity_suffix_start(source, marker_start, marker_end, tokens, identity_spans)
        .unwrap_or(marker_start)
}

fn neutral_identity_suffix_start(
    source: &[char],
    marker_start: usize,
    marker_end: usize,
    tokens: &[Token],
    identity_spans: &[Range<usize>],
) -> Option<usize> {
    let boundary_start =
        (marker_start..marker_end).find(|index| is_word_boundary(source[*index]))?;
    let mut suffix_start = boundary_start;
    while suffix_start < marker_end && is_word_boundary(source[suffix_start]) {
        suffix_start += 1;
    }
    if suffix_start == marker_end
        || !is_neutral_identity_range(&(suffix_start..marker_end), tokens, identity_spans)
    {
        return None;
    }

    let prefix: String = source[marker_start..boundary_start].iter().collect();
    (!is_inappropriate(&prefix)).then_some(suffix_start)
}

fn tokenize(characters: &[char]) -> Vec<Token> {
    let mut tokens = Vec::new();
    let mut index = 0;

    while index < characters.len() {
        if !is_compatibility_alphanumeric(characters[index]) {
            index += 1;
            continue;
        }

        let start = index;
        index += 1;
        while index < characters.len() && is_token_continuation(characters, index) {
            if is_substitution_separator(characters[index])
                && should_split_contextual_separator(characters, start, index)
            {
                break;
            }
            index += 1;
        }
        let text = canonical_token_text(&characters[start..index]);
        let (clause_boundary_before, tight_clause_boundary_before, direct_label_boundary_before) =
            tokens
                .last()
                .map(|previous: &Token| {
                    let separator = &characters[previous.end..start];
                    let boundary = has_clause_boundary(separator);
                    let tight =
                        boundary && !separator.iter().any(|character| character.is_whitespace());
                    let direct_label = boundary
                        && has_direct_label_separator(separator)
                        && !has_hard_clause_separator(separator);
                    (boundary, tight, direct_label)
                })
                .unwrap_or((false, false, false));
        tokens.push(Token {
            start,
            end: index,
            text,
            clause_boundary_before,
            tight_clause_boundary_before,
            direct_label_boundary_before,
        });
    }

    compact_contextual_evasions(tokens)
}

fn has_clause_boundary(separator: &[char]) -> bool {
    separator.iter().any(|character| {
        is_clause_punctuation(*character)
            || std::iter::once(*character)
                .nfkc()
                .any(is_clause_punctuation)
    })
}

fn has_hard_clause_separator(separator: &[char]) -> bool {
    separator.iter().any(|character| {
        is_hard_clause_punctuation(*character)
            || std::iter::once(*character)
                .nfkc()
                .any(is_hard_clause_punctuation)
    })
}

fn has_direct_label_separator(separator: &[char]) -> bool {
    separator.iter().any(|character| {
        is_direct_label_punctuation(*character)
            || std::iter::once(*character)
                .nfkc()
                .any(is_direct_label_punctuation)
    })
}

fn is_direct_label_punctuation(character: char) -> bool {
    matches!(character, ',' | ':' | '\u{060c}' | '\u{ff0c}' | '\u{ff1a}')
}

fn is_hard_clause_punctuation(character: char) -> bool {
    matches!(
        character,
        '\n' | '\r'
            | '.'
            | '!'
            | '?'
            | ';'
            | '\u{061b}'
            | '\u{061f}'
            | '\u{2026}'
            | '\u{3002}'
            | '\u{ff01}'
            | '\u{ff0e}'
            | '\u{ff1b}'
            | '\u{ff1f}'
    )
}

fn is_clause_punctuation(character: char) -> bool {
    character.is_ascii_punctuation()
        || matches!(
            character,
            '\n' | '\r'
                | '\u{060c}'
                | '\u{061b}'
                | '\u{061f}'
                | '\u{2013}'
                | '\u{2014}'
                | '\u{2026}'
                | '\u{3001}'
                | '\u{3002}'
                | '\u{ff0c}'
                | '\u{ff01}'
                | '\u{ff0e}'
                | '\u{ff0f}'
                | '\u{ff1a}'
                | '\u{ff1b}'
                | '\u{ff1f}'
        )
}

fn is_substitution_separator(character: char) -> bool {
    matches!(character, '@' | '$' | '!' | '|')
}

fn should_split_contextual_separator(
    characters: &[char],
    token_start: usize,
    separator_index: usize,
) -> bool {
    let prefix = canonical_token_text(&characters[token_start..separator_index]);
    if is_contextual_keyword(&prefix) || is_human_group_noun(&prefix) {
        return true;
    }

    let mut suffix_end = separator_index + 1;
    while suffix_end < characters.len()
        && (is_compatibility_alphanumeric(characters[suffix_end])
            || is_ignorable_token_character(characters[suffix_end]))
    {
        suffix_end += 1;
    }
    let suffix = canonical_token_text(&characters[separator_index + 1..suffix_end]);
    is_contextual_keyword(&suffix)
}

fn is_token_continuation(characters: &[char], index: usize) -> bool {
    let character = characters[index];
    is_compatibility_alphanumeric(character)
        || (is_apostrophe(character)
            && characters
                .get(index + 1)
                .is_some_and(|next| is_compatibility_alphanumeric(*next)))
        || is_ignorable_token_character(character)
        || (is_substitution_separator(character)
            && characters
                .get(index + 1)
                .is_some_and(|next| is_compatibility_alphanumeric(*next)))
}

fn is_compatibility_alphanumeric(character: char) -> bool {
    character.is_alphanumeric()
        || std::iter::once(character)
            .nfkc()
            .any(|normalized| normalized.is_alphanumeric())
}

fn is_apostrophe(character: char) -> bool {
    matches!(character, '\'' | '\u{2018}' | '\u{2019}' | '\u{02bc}')
}

fn is_ignorable_token_character(character: char) -> bool {
    matches!(character.get_minor_category(), MinorCategory::Cf)
}

fn canonical_token_text(characters: &[char]) -> String {
    let normalized: String = characters
        .iter()
        .filter_map(|&character| {
            if is_ignorable_token_character(character) {
                None
            } else if is_apostrophe(character) {
                Some('\'')
            } else {
                Some(character)
            }
        })
        .nfkc()
        .collect();
    if !normalized.is_empty()
        && normalized
            .chars()
            .all(|character| character.is_ascii_digit())
    {
        return normalized;
    }
    let mut text = String::with_capacity(normalized.len());

    for character in normalized.chars() {
        for lowercase in character.to_lowercase() {
            text.push(canonical_token_character(lowercase));
        }
    }

    text
}

fn compact_contextual_evasions(tokens: Vec<Token>) -> Vec<Token> {
    let mut compacted = Vec::with_capacity(tokens.len());
    let mut index = 0;

    while index < tokens.len() {
        let mut candidate = String::new();
        let mut best_match = None;

        for (end, token) in tokens.iter().enumerate().skip(index).take(16) {
            if candidate.chars().count() + token.text.chars().count() > 24 {
                break;
            }
            candidate.push_str(&token.text);
            if is_contextual_keyword(&candidate) {
                best_match = Some((end + 1, candidate.clone()));
            }
        }

        if let Some((end, text)) = best_match {
            compacted.push(Token {
                start: tokens[index].start,
                end: tokens[end - 1].end,
                text,
                clause_boundary_before: tokens[index].clause_boundary_before,
                tight_clause_boundary_before: tokens[index].tight_clause_boundary_before,
                direct_label_boundary_before: tokens[index].direct_label_boundary_before,
            });
            index = end;
        } else {
            compacted.push(tokens[index].clone());
            index += 1;
        }
    }

    compacted
}

fn is_contextual_keyword(token: &str) -> bool {
    is_identity_word(token)
        || is_hostile_action(token)
        || is_hostile_label_or_outcome(token)
        || is_counter_action(token)
}

fn clause_start(tokens: &[Token], index: usize) -> usize {
    (0..=index)
        .rev()
        .find(|candidate| tokens[*candidate].clause_boundary_before)
        .unwrap_or(0)
}

fn hard_clause_start(tokens: &[Token], index: usize) -> usize {
    (0..=index)
        .rev()
        .find(|candidate| {
            tokens[*candidate].clause_boundary_before
                && !tokens[*candidate].tight_clause_boundary_before
        })
        .unwrap_or(0)
}

fn canonical_token_character(character: char) -> char {
    match character {
        '0' | 'о' | 'ο' => 'o',
        '1' | '!' | 'і' | 'ӏ' | 'ı' | 'ɩ' => 'i',
        '2' => 'z',
        '3' | 'е' | 'ε' => 'e',
        '4' | '@' | 'а' | 'α' => 'a',
        '5' | '$' | 'ѕ' => 's',
        '6' => 'g',
        '7' => 't',
        '8' => 'b',
        '9' | 'ɡ' => 'g',
        'с' => 'c',
        'ј' => 'j',
        'к' => 'k',
        'м' => 'm',
        'н' => 'h',
        'р' | 'ρ' => 'p',
        'т' => 't',
        'у' | 'υ' => 'y',
        'х' | 'χ' => 'x',
        'в' => 'b',
        'ᴀ' => 'a',
        'ʙ' => 'b',
        'ᴄ' => 'c',
        'ᴅ' => 'd',
        'ᴇ' => 'e',
        'ꜰ' => 'f',
        'ɢ' => 'g',
        'ʜ' => 'h',
        'ɪ' => 'i',
        'ᴊ' => 'j',
        'ᴋ' => 'k',
        'ʟ' => 'l',
        'ᴍ' => 'm',
        'ɴ' => 'n',
        'ᴏ' => 'o',
        'ᴘ' => 'p',
        'ʀ' => 'r',
        'ꜱ' => 's',
        'ᴛ' => 't',
        'ᴜ' => 'u',
        'ᴠ' => 'v',
        'ᴡ' => 'w',
        'ʏ' => 'y',
        'ᴢ' => 'z',
        '|' => 'l',
        _ => character,
    }
}

fn identity_token_spans(tokens: &[Token]) -> Vec<Range<usize>> {
    let mut spans = Vec::new();
    let mut index = 0;

    while index < tokens.len() {
        let Some(end) = identity_span_end(tokens, index) else {
            index += 1;
            continue;
        };
        spans.push(index..end);
        index = end;
    }

    spans
}

fn identity_span_end(tokens: &[Token], index: usize) -> Option<usize> {
    let token = tokens[index].text.as_str();
    let group_noun = tokens
        .get(index + 1)
        .map(|next| is_human_group_noun(&next.text))
        .unwrap_or(false);

    if token == "homo" {
        return tokens
            .get(index + 1)
            .filter(|next| next.text == "sapiens")
            .map(|_| index + 2);
    }

    if matches!(token, "people" | "person")
        && tokens.get(index + 1).is_some_and(|next| next.text == "of")
        && tokens
            .get(index + 2)
            .is_some_and(|next| next.text == "color")
    {
        return Some(index + 3);
    }

    is_identity_word(token).then_some(index + 1 + usize::from(group_noun))
}

fn is_human_group_noun(token: &str) -> bool {
    matches!(
        token,
        "people"
            | "person"
            | "man"
            | "men"
            | "woman"
            | "women"
            | "child"
            | "children"
            | "kid"
            | "kids"
            | "community"
            | "communities"
            | "folk"
            | "folks"
            | "guy"
            | "guys"
            | "gamer"
            | "gamers"
            | "player"
            | "players"
            | "user"
            | "users"
    )
}

fn is_identity_word(token: &str) -> bool {
    matches!(
        token,
        "gay"
            | "gays"
            | "lesbian"
            | "lesbians"
            | "queer"
            | "queers"
            | "jew"
            | "jews"
            | "jewish"
            | "trans"
            | "transgender"
            | "transgenders"
            | "transfem"
            | "transmasc"
            | "bisexual"
            | "bisexuals"
            | "homosexual"
            | "homosexuals"
            | "lgbt"
            | "lgbtq"
            | "lgbtqi"
            | "lgbtqa"
            | "lgbtqia"
            | "lgbtqia2s"
            | "nonbinary"
            | "intersex"
            | "asexual"
            | "asexuals"
            | "atheist"
            | "atheists"
            | "pansexual"
            | "pansexuals"
            | "straight"
            | "muslim"
            | "muslims"
            | "christian"
            | "christians"
            | "chinese"
            | "israeli"
            | "israelis"
            | "hindu"
            | "hindus"
            | "buddhist"
            | "buddhists"
            | "sikh"
            | "sikhs"
            | "asian"
            | "asians"
            | "latino"
            | "latinos"
            | "latina"
            | "latinas"
            | "hispanic"
            | "hispanics"
            | "arab"
            | "arabs"
            | "indigenous"
            | "roma"
            | "romani"
            | "black"
            | "white"
            | "disabled"
            | "autistic"
            | "neurodivergent"
            | "immigrant"
            | "immigrants"
            | "migrant"
            | "migrants"
            | "refugee"
            | "refugees"
            | "palestinian"
            | "palestinians"
            | "woman"
            | "women"
            | "man"
            | "men"
            | "female"
            | "females"
            | "male"
            | "males"
    )
}

fn is_neutral_identity_range(
    character_range: &Range<usize>,
    tokens: &[Token],
    identity_spans: &[Range<usize>],
) -> bool {
    identity_spans.iter().any(|identity_span| {
        let first = &tokens[identity_span.start];
        character_range.start == first.start
            && character_range.end <= tokens[identity_span.end - 1].end
    })
}

fn contextual_hate_ranges(tokens: &[Token], identity_spans: &[Range<usize>]) -> Vec<Range<usize>> {
    let mut ranges = Vec::new();

    for identity_span in identity_spans {
        let identity_start = identity_span.start;
        let identity_end = identity_span.end;
        let human_target = is_contextual_human_target(tokens, identity_span);
        let mut matched_post_hostility = false;

        if human_target {
            let action_search_start = identity_start.saturating_sub(16);
            for action_index in (action_search_start..identity_start).rev() {
                if !is_hostile_action(&tokens[action_index].text) {
                    continue;
                }
                if is_competitive_game_action(tokens, action_index, identity_span) {
                    continue;
                }
                let bridge = &tokens[action_index + 1..identity_start];
                if crosses_hard_clause_boundary(tokens, action_index + 1, identity_start + 1)
                    && !is_cross_clause_targeting_continuation(&tokens[action_index].text, bridge)
                {
                    continue;
                }
                if is_targeting_bridge(bridge)
                    && !action_is_countered(tokens, action_index, identity_end)
                {
                    ranges.push(tokens[action_index].start..tokens[action_index].end);
                    break;
                }
            }
        }

        let post_end = (identity_end + 8).min(tokens.len());
        for hostile_index in identity_end..post_end {
            if !is_hostile_label_or_outcome(&tokens[hostile_index].text) {
                continue;
            }
            let hostile = tokens[hostile_index].text.as_str();
            let bridge = &tokens[identity_end..hostile_index];
            let predicate_starts_after_hard_boundary = bridge.first().is_some_and(|token| {
                token.clause_boundary_before && !token.tight_clause_boundary_before
            });
            if predicate_starts_after_hard_boundary
                || bridge.is_empty()
                    && crosses_hard_clause_boundary(tokens, identity_end, hostile_index + 1)
                    && !tokens[hostile_index].direct_label_boundary_before
            {
                continue;
            }
            let descriptor_target = is_unambiguous_descriptor_insult(
                tokens,
                identity_span,
                identity_end,
                hostile_index,
            );
            let hostile_predicate = if matches!(hostile, "allowed" | "belong") {
                is_exclusion_predicate(tokens, identity_start, identity_end, hostile_index)
            } else {
                is_hostile_predicate(tokens, identity_end, hostile_index)
                    || is_hostile_wish(tokens, identity_start, identity_end, hostile_index)
            };
            if (human_target || descriptor_target)
                && hostile_predicate
                && !hostile_context_is_countered(tokens, identity_start)
                && !hostile_claim_is_refuted(tokens, hostile_index)
            {
                ranges.push(tokens[hostile_index].start..tokens[hostile_index].end);
                matched_post_hostility = true;
                break;
            }
        }

        if human_target
            && matched_post_hostility
            && let Some(pronoun_range) = adjacent_pronoun_hate_range(tokens, identity_end)
        {
            ranges.push(pronoun_range);
        }
    }

    ranges
}

fn crosses_hard_clause_boundary(tokens: &[Token], start: usize, end: usize) -> bool {
    tokens[start..end]
        .iter()
        .any(|token| token.clause_boundary_before && !token.tight_clause_boundary_before)
}

fn is_cross_clause_targeting_continuation(action: &str, bridge: &[Token]) -> bool {
    bridge.iter().any(|token| {
        token
            .text
            .chars()
            .all(|character| character.is_ascii_digit())
            || matches!(
                token.text.as_str(),
                "all"
                    | "any"
                    | "both"
                    | "each"
                    | "every"
                    | "few"
                    | "many"
                    | "most"
                    | "several"
                    | "some"
                    | "two"
            )
    }) || action == "death" && bridge.iter().any(|token| token.text == "to")
}

fn is_competitive_game_action(
    tokens: &[Token],
    action_index: usize,
    identity_span: &Range<usize>,
) -> bool {
    let competitive_verb = matches!(
        tokens[action_index].text.as_str(),
        "beat" | "beating" | "beats" | "beaten"
    );
    let player_target = matches!(
        tokens[identity_span.end - 1].text.as_str(),
        "guy" | "guys" | "player" | "players"
    );
    let prefix_start = action_index
        .saturating_sub(4)
        .max(hard_clause_start(tokens, action_index));
    let prefix = &tokens[prefix_start..action_index];
    let bridge = &tokens[action_index + 1..identity_span.start];
    competitive_verb
        && player_target
        && !contains_physical_violence_marker(prefix)
        && !contains_physical_violence_marker(bridge)
        && !has_physical_violence_continuation(tokens, identity_span.end)
}

fn has_physical_violence_continuation(tokens: &[Token], start: usize) -> bool {
    let maximum_end = (start + 8).min(tokens.len());
    let tail_end = (start..maximum_end)
        .find(|index| {
            tokens[*index].clause_boundary_before && !tokens[*index].tight_clause_boundary_before
        })
        .unwrap_or(maximum_end);
    contains_physical_violence_marker(&tokens[start..tail_end])
}

fn contains_physical_violence_marker(tokens: &[Token]) -> bool {
    tokens.iter().any(|token| {
        matches!(
            token.text.as_str(),
            "bat"
                | "attack"
                | "attacked"
                | "attacking"
                | "attacks"
                | "bats"
                | "bleed"
                | "bleeding"
                | "blood"
                | "brutally"
                | "dead"
                | "death"
                | "die"
                | "dying"
                | "fist"
                | "fists"
                | "injure"
                | "injured"
                | "injuring"
                | "irl"
                | "moving"
                | "physically"
                | "pulp"
                | "senseless"
                | "unconscious"
                | "up"
                | "violently"
                | "weapon"
                | "weapons"
        )
    }) || tokens
        .windows(2)
        .any(|window| window[0].text == "real" && window[1].text == "life")
}

fn adjacent_pronoun_hate_range(tokens: &[Token], identity_end: usize) -> Option<Range<usize>> {
    let search_end = (identity_end + 10).min(tokens.len());
    let pronoun_index =
        (identity_end..search_end).find(|index| tokens[*index].clause_boundary_before)?;
    if tokens[pronoun_index].text != "they" {
        return None;
    }

    let hostile_index = (pronoun_index + 1..search_end)
        .take_while(|index| !tokens[*index].clause_boundary_before)
        .find(|index| is_hostile_label_or_outcome(&tokens[*index].text))?;
    if !is_hostile_predicate(tokens, pronoun_index + 1, hostile_index)
        || hostile_claim_is_refuted(tokens, hostile_index)
    {
        return None;
    }

    Some(tokens[hostile_index].start..tokens[hostile_index].end)
}

fn is_contextual_human_target(tokens: &[Token], identity_span: &Range<usize>) -> bool {
    if identity_span.end - identity_span.start > 1 {
        return true;
    }

    !matches!(
        tokens[identity_span.start].text.as_str(),
        "arab"
            | "asian"
            | "asexual"
            | "autistic"
            | "bisexual"
            | "black"
            | "buddhist"
            | "christian"
            | "chinese"
            | "disabled"
            | "female"
            | "gay"
            | "hindu"
            | "hispanic"
            | "homosexual"
            | "indigenous"
            | "intersex"
            | "israeli"
            | "jewish"
            | "latina"
            | "latino"
            | "lesbian"
            | "lgbt"
            | "lgbtq"
            | "lgbtqi"
            | "lgbtqa"
            | "lgbtqia"
            | "lgbtqia2s"
            | "male"
            | "muslim"
            | "neurodivergent"
            | "nonbinary"
            | "pansexual"
            | "palestinian"
            | "queer"
            | "romani"
            | "sikh"
            | "straight"
            | "trans"
            | "transfem"
            | "transgender"
            | "transmasc"
            | "white"
    )
}

fn is_unambiguous_descriptor_insult(
    tokens: &[Token],
    identity_span: &Range<usize>,
    identity_end: usize,
    hostile_index: usize,
) -> bool {
    identity_span.end - identity_span.start == 1
        && hostile_index == identity_end
        && matches!(
            tokens[identity_span.start].text.as_str(),
            "arab"
                | "asian"
                | "asexual"
                | "autistic"
                | "bisexual"
                | "buddhist"
                | "christian"
                | "chinese"
                | "gay"
                | "hindu"
                | "hispanic"
                | "homosexual"
                | "indigenous"
                | "intersex"
                | "israeli"
                | "jewish"
                | "latina"
                | "latino"
                | "lesbian"
                | "lgbt"
                | "lgbtq"
                | "lgbtqi"
                | "lgbtqa"
                | "lgbtqia"
                | "lgbtqia2s"
                | "muslim"
                | "neurodivergent"
                | "nonbinary"
                | "pansexual"
                | "palestinian"
                | "queer"
                | "romani"
                | "sikh"
                | "trans"
                | "transfem"
                | "transgender"
                | "transmasc"
        )
        && matches!(
            tokens[hostile_index].text.as_str(),
            "ape"
                | "apes"
                | "disgusting"
                | "filth"
                | "freak"
                | "freaks"
                | "inferior"
                | "monkey"
                | "monkeys"
                | "scum"
                | "subhuman"
                | "trash"
                | "vermin"
        )
}

fn is_targeting_bridge(tokens: &[Token]) -> bool {
    tokens
        .iter()
        .all(|token| is_targeting_bridge_modifier(&token.text))
}

fn is_targeting_bridge_modifier(token: &str) -> bool {
    token.chars().all(|character| character.is_ascii_digit())
        || token.ends_with("ly")
        || matches!(
            token,
            "a" | "all"
                | "and"
                | "any"
                | "awful"
                | "called"
                | "couple"
                | "both"
                | "damn"
                | "damned"
                | "dirty"
                | "each"
                | "entire"
                | "every"
                | "few"
                | "filthy"
                | "goddamn"
                | "horrible"
                | "last"
                | "local"
                | "many"
                | "most"
                | "of"
                | "one"
                | "ones"
                | "out"
                | "our"
                | "single"
                | "several"
                | "some"
                | "so"
                | "stupid"
                | "the"
                | "their"
                | "that"
                | "them"
                | "these"
                | "those"
                | "to"
                | "two"
                | "up"
                | "whole"
                | "your"
        )
}

fn action_is_countered(tokens: &[Token], action_index: usize, identity_end: usize) -> bool {
    let wide_prefix_start = action_index.saturating_sub(8);
    let wide_prefix = &tokens[wide_prefix_start..action_index];
    let prefix_start = action_index
        .saturating_sub(6)
        .max(clause_start(tokens, action_index));
    let prefix = &tokens[prefix_start..action_index];

    let immediately_negated = wide_prefix.last().is_some_and(|token| {
        matches!(
            token.text.as_str(),
            "not"
                | "never"
                | "dont"
                | "don't"
                | "cant"
                | "can't"
                | "shouldnt"
                | "shouldn't"
                | "wouldnt"
                | "wouldn't"
        )
    });
    let no_one_directive = has_no_one_directive(tokens, wide_prefix_start, action_index);
    let question_or_condemnation = prefix.iter().enumerate().any(|(offset, token)| {
        let index = prefix_start + offset;
        (token.text == "why" && is_question_bridge(&tokens[index + 1..action_index]))
            || (matches!(
                token.text.as_str(),
                "awful" | "terrible" | "unacceptable" | "wrong"
            ) && tokens.get(index + 1).is_some_and(|next| next.text == "to")
                && index + 2 == action_index)
            || (is_counter_action(&token.text)
                && !counter_action_is_negated(tokens, index)
                && tokens[index + 1..action_index]
                    .iter()
                    .all(|token| is_counter_action_bridge(&token.text)))
    });

    immediately_negated
        || has_extended_negation(wide_prefix)
        || has_negated_desire(wide_prefix)
        || has_negated_permission(wide_prefix)
        || no_one_directive
        || question_or_condemnation
        || has_counter_conclusion(tokens, identity_end)
}

fn has_negated_permission(prefix: &[Token]) -> bool {
    prefix.iter().enumerate().any(|(index, token)| {
        matches!(
            token.text.as_str(),
            "cannot" | "cant" | "can't" | "dont" | "don't" | "wont" | "won't"
        ) && prefix
            .get(index + 1)
            .is_some_and(|next| next.text == "allow")
            && prefix[index + 2..].iter().all(|trailing| {
                matches!(
                    trailing.text.as_str(),
                    "anyone" | "people" | "person" | "someone" | "them" | "to"
                )
            })
    })
}

fn has_extended_negation(prefix: &[Token]) -> bool {
    prefix.iter().enumerate().any(|(index, token)| {
        matches!(token.text.as_str(), "not" | "never")
            && prefix[index + 1..].iter().all(|trailing| {
                matches!(
                    trailing.text.as_str(),
                    "absolutely"
                        | "all"
                        | "any"
                        | "circumstance"
                        | "circumstances"
                        | "ever"
                        | "please"
                        | "under"
                )
            })
    })
}

fn has_negated_desire(prefix: &[Token]) -> bool {
    let texts: Vec<&str> = prefix.iter().map(|token| token.text.as_str()).collect();
    texts.ends_with(&["dont", "want", "to"])
        || texts.ends_with(&["don't", "want", "to"])
        || texts.ends_with(&["do", "not", "want", "to"])
        || texts.ends_with(&["never", "want", "to"])
        || texts.ends_with(&["wouldnt", "want", "to"])
        || texts.ends_with(&["wouldn't", "want", "to"])
}

fn has_no_one_directive(tokens: &[Token], start: usize, action_index: usize) -> bool {
    for index in start..action_index {
        let subject_end = if matches!(tokens[index].text.as_str(), "nobody" | "noone" | "none") {
            index + 1
        } else if tokens[index].text == "no"
            && tokens.get(index + 1).is_some_and(|next| next.text == "one")
        {
            index + 2
        } else {
            continue;
        };

        if tokens[subject_end..action_index].iter().all(|token| {
            matches!(
                token.text.as_str(),
                "can" | "could" | "ever" | "must" | "ought" | "should" | "to" | "would"
            )
        }) {
            return true;
        }
    }

    false
}

fn is_question_bridge(tokens: &[Token]) -> bool {
    tokens.iter().all(|token| {
        matches!(
            token.text.as_str(),
            "anyone"
                | "can"
                | "could"
                | "did"
                | "do"
                | "does"
                | "people"
                | "person"
                | "should"
                | "someone"
                | "they"
                | "we"
                | "would"
                | "you"
        )
    })
}

fn is_counter_action_bridge(token: &str) -> bool {
    matches!(
        token,
        "anyone"
            | "attack"
            | "attacks"
            | "being"
            | "call"
            | "called"
            | "calling"
            | "from"
            | "harm"
            | "label"
            | "labeled"
            | "labeling"
            | "people"
            | "person"
            | "phrase"
            | "say"
            | "said"
            | "says"
            | "that"
            | "the"
            | "those"
            | "threat"
            | "threats"
            | "to"
            | "users"
            | "violence"
            | "who"
    )
}

fn is_counter_action(token: &str) -> bool {
    matches!(
        token,
        "avoid"
            | "avoids"
            | "condemn"
            | "condemns"
            | "condemned"
            | "oppose"
            | "opposes"
            | "opposed"
            | "prevent"
            | "prevents"
            | "prevented"
            | "protect"
            | "protects"
            | "protected"
            | "report"
            | "reports"
            | "reported"
            | "stop"
            | "stops"
            | "stopped"
    )
}

fn counter_action_is_negated(tokens: &[Token], index: usize) -> bool {
    index > 0
        && matches!(
            tokens[index - 1].text.as_str(),
            "not" | "dont" | "don't" | "never"
        )
}

fn has_counter_conclusion(tokens: &[Token], start: usize) -> bool {
    if start >= tokens.len() {
        return false;
    }
    let mut end = (start + 4).min(tokens.len());
    if let Some(boundary) = (start + 1..end).find(|index| tokens[*index].clause_boundary_before) {
        end = boundary;
    }
    let tail = &tokens[start..end];
    matches!(
        tail,
        [copula, judgment, ..]
            if is_copula(&copula.text) && is_counter_judgment(&judgment.text)
    ) || matches!(
        tail,
        [conjunction, copula, judgment, ..]
            if matches!(conjunction.text.as_str(), "and" | "which")
                && is_copula(&copula.text)
                && is_counter_judgment(&judgment.text)
    ) || matches!(
        tail,
        [modal, counter, ..]
            if matches!(modal.text.as_str(), "must" | "should")
                && matches!(counter.text.as_str(), "stop" | "end")
    )
}

fn is_counter_judgment(token: &str) -> bool {
    matches!(
        token,
        "abhorrent" | "false" | "hateful" | "illegal" | "unacceptable" | "untrue" | "wrong"
    )
}

fn is_hostile_predicate(tokens: &[Token], identity_end: usize, hostile_index: usize) -> bool {
    let hostile = tokens[hostile_index].text.as_str();
    let bridge = &tokens[identity_end..hostile_index];

    if is_passive_hostile_outcome(hostile) {
        return is_hostile_modal_bridge(bridge);
    }

    if bridge.is_empty() {
        return !matches!(hostile, "cancer" | "disease");
    }

    let direct_predicate = is_predicate_copula(&bridge[0].text)
        && bridge[1..]
            .iter()
            .all(|token| is_predicate_modifier(&token.text));
    let anything_but_predicate = matches!(
        bridge,
        [modal, never, be, anything, but]
            if matches!(modal.text.as_str(), "can" | "could")
                && never.text == "never"
                && be.text == "be"
                && anything.text == "anything"
                && but.text == "but"
    );
    let intensifying_negation_predicate = matches!(
        bridge,
        [copula, not, merely]
            if is_copula(&copula.text) && not.text == "not" && merely.text == "merely"
    );
    let enduring_predicate = matches!(
        bridge,
        [modal, never, stop, being]
            if matches!(modal.text.as_str(), "can" | "could" | "will" | "would")
                && never.text == "never"
                && stop.text == "stop"
                && being.text == "being"
    );
    let repeated_subject_predicate = bridge
        .last()
        .is_some_and(|token| is_pronoun_copula(&token.text))
        || bridge.len() >= 2
            && bridge[bridge.len() - 2].text == "they"
            && is_copula(&bridge[bridge.len() - 1].text);
    let denied_equality_predicate = matches!(
        bridge,
        [copula, not, equal, and, second_copula]
            if is_copula(&copula.text)
                && not.text == "not"
                && equal.text == "equal"
                && and.text == "and"
                && is_copula(&second_copula.text)
    );

    direct_predicate
        || anything_but_predicate
        || repeated_subject_predicate
        || denied_equality_predicate
        || intensifying_negation_predicate
        || enduring_predicate
}

fn is_hostile_wish(
    tokens: &[Token],
    identity_start: usize,
    identity_end: usize,
    hostile_index: usize,
) -> bool {
    let prefix = &tokens[identity_start.saturating_sub(3)..identity_start];
    let bridge = &tokens[identity_end..hostile_index];
    prefix.iter().any(|token| {
        matches!(
            token.text.as_str(),
            "hope" | "hopes" | "hoped" | "wish" | "wishes" | "wished"
        )
    }) && (bridge.is_empty()
        || is_predicate_copula(&bridge[0].text)
            && bridge[1..]
                .iter()
                .all(|token| is_predicate_modifier(&token.text)))
}

fn is_exclusion_predicate(
    tokens: &[Token],
    identity_start: usize,
    identity_end: usize,
    hostile_index: usize,
) -> bool {
    let prefixed_with_no = identity_start > 0
        && tokens[identity_start - 1].text == "no"
        && !tokens[identity_start].clause_boundary_before;
    let bridge = &tokens[identity_end..hostile_index];
    let explicitly_disallowed = matches!(
        bridge,
        [copula, not]
            if is_copula(&copula.text) && matches!(not.text.as_str(), "not" | "never")
    ) || matches!(
        bridge,
        [negation]
            if matches!(
                negation.text.as_str(),
                "arent" | "aren't" | "dont" | "don't" | "never"
            )
    ) || matches!(
        bridge,
        [auxiliary, not]
            if matches!(auxiliary.text.as_str(), "do" | "does") && not.text == "not"
    );
    prefixed_with_no || explicitly_disallowed
}

fn hostile_context_is_countered(tokens: &[Token], identity_start: usize) -> bool {
    let prefix_start = identity_start
        .saturating_sub(8)
        .max(clause_start(tokens, identity_start));
    let prefix = &tokens[prefix_start..identity_start];
    if matches!(
        prefix,
        [.., judgment, that]
            if is_counter_judgment(&judgment.text) && that.text == "that"
    ) {
        return true;
    }
    if matches!(
        prefix,
        [.., judgment, to, reporting]
            if is_counter_judgment(&judgment.text)
                && to.text == "to"
                && matches!(reporting.text.as_str(), "say" | "says" | "claim" | "call")
    ) {
        return true;
    }

    if (prefix_start..identity_start).any(|index| {
        is_counter_action(&tokens[index].text)
            && !counter_action_is_negated(tokens, index)
            && tokens[index + 1..identity_start]
                .iter()
                .all(|token| is_counter_action_bridge(&token.text))
    }) {
        return true;
    }

    for index in prefix_start..identity_start {
        if !matches!(
            tokens[index].text.as_str(),
            "call" | "called" | "calling" | "label" | "labeled" | "labeling"
        ) {
            continue;
        }
        let immediately_negated = index > 0
            && matches!(
                tokens[index - 1].text.as_str(),
                "dont" | "don't" | "never" | "not" | "shouldnt" | "shouldn't"
            );
        if immediately_negated || has_no_one_directive(tokens, index.saturating_sub(6), index) {
            return true;
        }
    }

    false
}

fn is_copula(token: &str) -> bool {
    matches!(token, "am" | "are" | "be" | "is" | "was" | "were")
}

fn is_predicate_copula(token: &str) -> bool {
    is_copula(token)
        || matches!(
            token,
            "became" | "become" | "becomes" | "remain" | "remains" | "seem" | "seems"
        )
}

fn is_pronoun_copula(token: &str) -> bool {
    matches!(token, "theyre" | "they're")
}

fn is_predicate_modifier(token: &str) -> bool {
    token.ends_with("ly")
        || matches!(
            token,
            "a" | "all"
                | "an"
                | "but"
                | "brutal"
                | "filthy"
                | "just"
                | "most"
                | "nothing"
                | "parasitic"
                | "pure"
                | "savage"
                | "so"
                | "the"
                | "too"
                | "very"
                | "violent"
        )
}

fn is_hostile_modal_bridge(bridge: &[Token]) -> bool {
    !bridge.is_empty()
        && bridge.iter().any(|token| {
            matches!(
                token.text.as_str(),
                "deserve" | "deserves" | "must" | "need" | "needs" | "ought" | "should"
            )
        })
        && bridge.iter().all(|token| {
            matches!(
                token.text.as_str(),
                "all"
                    | "be"
                    | "deserve"
                    | "deserves"
                    | "must"
                    | "need"
                    | "needs"
                    | "ought"
                    | "should"
                    | "to"
            )
        })
}

fn hostile_claim_is_refuted(tokens: &[Token], hostile_index: usize) -> bool {
    has_counter_conclusion(tokens, hostile_index + 1)
}

fn is_hostile_action(token: &str) -> bool {
    matches!(
        token,
        "hate"
            | "hates"
            | "hated"
            | "hating"
            | "despise"
            | "despises"
            | "despised"
            | "attack"
            | "attacks"
            | "attacked"
            | "attacking"
            | "ban"
            | "bans"
            | "banned"
            | "banning"
            | "beat"
            | "beats"
            | "beaten"
            | "beating"
            | "burn"
            | "burns"
            | "burned"
            | "burning"
            | "cleanse"
            | "cleanses"
            | "cleansed"
            | "cleansing"
            | "death"
            | "driveout"
            | "exclude"
            | "excludes"
            | "excluded"
            | "excluding"
            | "expel"
            | "expels"
            | "expelled"
            | "expelling"
            | "kill"
            | "kills"
            | "killed"
            | "killing"
            | "murder"
            | "murders"
            | "murdered"
            | "murdering"
            | "hang"
            | "hangs"
            | "hanged"
            | "hanging"
            | "lynch"
            | "lynches"
            | "lynched"
            | "lynching"
            | "purge"
            | "purges"
            | "purged"
            | "purging"
            | "stab"
            | "stabs"
            | "stabbed"
            | "stabbing"
            | "execute"
            | "executes"
            | "executed"
            | "executing"
            | "torture"
            | "tortures"
            | "tortured"
            | "torturing"
            | "shoot"
            | "shoots"
            | "shooting"
            | "shot"
            | "slaughter"
            | "slaughters"
            | "slaughtered"
            | "slaughtering"
            | "sterilize"
            | "sterilizes"
            | "sterilized"
            | "sterilizing"
            | "wipe"
            | "wipes"
            | "wiped"
            | "wiping"
            | "wipeout"
            | "gas"
            | "gassed"
            | "gassing"
            | "gaz"
            | "exterminate"
            | "exterminates"
            | "exterminated"
            | "exterminating"
            | "eradicate"
            | "eradicates"
            | "eradicated"
            | "eradicating"
            | "deport"
            | "deports"
            | "deported"
            | "deporting"
            | "eliminate"
            | "eliminates"
            | "eliminated"
            | "eliminating"
    )
}

fn is_hostile_label_or_outcome(token: &str) -> bool {
    matches!(
        token,
        "vermin"
            | "monkey"
            | "monkeys"
            | "monster"
            | "monsters"
            | "parasite"
            | "parasites"
            | "ape"
            | "apes"
            | "scum"
            | "disgusting"
            | "inferior"
            | "evil"
            | "subhuman"
            | "disease"
            | "cancer"
            | "freak"
            | "freaks"
            | "trash"
            | "filth"
            | "die"
            | "dead"
            | "killed"
            | "murdered"
            | "gassed"
            | "exterminated"
            | "eradicated"
            | "deported"
            | "eliminated"
            | "allowed"
            | "belong"
            | "perish"
    )
}

fn is_passive_hostile_outcome(token: &str) -> bool {
    matches!(
        token,
        "dead"
            | "deported"
            | "die"
            | "eliminated"
            | "eradicated"
            | "exterminated"
            | "gassed"
            | "killed"
            | "murdered"
            | "perish"
    )
}

fn ranges_overlap(left: &Range<usize>, right: &Range<usize>) -> bool {
    left.start < right.end && right.start < left.end
}

fn match_starts_inside_word(characters: &[char], start: usize) -> bool {
    if start == 0 || !characters[start].is_alphanumeric() {
        return false;
    }

    characters[start - 1].is_alphanumeric()
        || (characters[start - 1] == '\'' && start >= 2 && characters[start - 2].is_alphanumeric())
}

/// Rustrict treats repeated boundary letters as an evasion, so `is shit` can
/// be detected as `s shit`. If a suffix after a real boundary is independently
/// inappropriate, mask that suffix and preserve the neighboring clean word.
fn direct_inappropriate_suffix(
    characters: &[char],
    detected_start: usize,
    detected_end: usize,
) -> Option<usize> {
    let boundary_start =
        (detected_start..detected_end).find(|index| is_word_boundary(characters[*index]))?;

    let mut suffix_start = boundary_start;
    while suffix_start < detected_end && is_word_boundary(characters[suffix_start]) {
        suffix_start += 1;
    }
    if suffix_start == detected_end {
        return None;
    }

    let mut prefix_start = detected_start;
    while prefix_start > 0 && !is_word_boundary(characters[prefix_start - 1]) {
        prefix_start -= 1;
    }

    let prefix: String = characters[prefix_start..boundary_start].iter().collect();
    let suffix: String = characters[suffix_start..detected_end].iter().collect();
    (!is_inappropriate(&prefix) && is_inappropriate(&suffix)).then_some(suffix_start)
}

fn is_inappropriate(message: &str) -> bool {
    Censor::from_str(message)
        .with_trie(&CHAT_FILTER_TRIE)
        .with_ignore_false_positives(false)
        .analyze()
        .is(Type::INAPPROPRIATE)
}

fn is_word_boundary(character: char) -> bool {
    !character.is_alphanumeric()
        && !is_apostrophe(character)
        && !is_ignorable_token_character(character)
}

fn mask_normalized_content(mut source: Vec<char>, mut filtered: Vec<char>) -> String {
    if source.len() != filtered.len() {
        for character in &mut filtered {
            if *character == CENSOR_MARKER {
                *character = '*';
            }
        }
        return filtered.into_iter().collect();
    }

    for range in content_mask_ranges(&source, &filtered) {
        for character in &mut source[range] {
            *character = '*';
        }
    }
    source.into_iter().collect()
}

fn is_bidi_control(character: char) -> bool {
    matches!(
        character,
        '\u{061c}'
            | '\u{200e}'
            | '\u{200f}'
            | '\u{202a}'..='\u{202e}'
            | '\u{2066}'..='\u{2069}'
    )
}

#[cfg(test)]
mod tests {
    use super::{filter_chat_message, normalize_with_mapping};
    use rustrict::{Censor, Type};

    #[test]
    fn masks_every_character_of_detected_words() {
        assert_eq!(filter_chat_message("that is shit"), "that is ****");
        assert_eq!(filter_chat_message("KKK"), "***");
        assert_eq!(filter_chat_message("shit and fuck"), "**** and ****");
    }

    #[test]
    fn preserves_neighboring_words_and_punctuation() {
        assert_eq!(filter_chat_message("is-shit"), "is-****");
        assert_eq!(filter_chat_message("is shit"), "is ****");
        assert_eq!(filter_chat_message("this-shit"), "this-****");
        assert_eq!(filter_chat_message("this.shit"), "this.****");
        assert_eq!(filter_chat_message("class-shit"), "class-****");
        assert_eq!(filter_chat_message("class/shit"), "class/****");
        assert_eq!(filter_chat_message("well, shit!"), "well, ****!");
        assert_eq!(
            filter_chat_message("glass a s s h o l e"),
            "glass *************"
        );
        assert_eq!(filter_chat_message("this\u{301}-shit"), "this\u{301}-****");
        assert_eq!(filter_chat_message("Cafe\u{301} shit"), "Cafe\u{301} ****");
        assert_eq!(filter_chat_message("か\u{3099} shit"), "か\u{3099} ****");
    }

    #[test]
    fn catches_common_obfuscation_methods() {
        for message in [
            "FUCK",
            "f u c k",
            "fuuuuuuuck",
            "fμ¢κ",
            "f.u-c_k",
            "fučk",
            "sh1t",
            "§uck",
            "f\u{200b}uck",
            "a s s h o l e",
            "a.s.s.h.o.l.e",
            "n.i.g.g.e.r",
            "ni gger",
            "r-e-t-a-r-d",
            "w_h_o_r_e",
        ] {
            let filtered = filter_chat_message(message);
            assert_ne!(
                filtered, message,
                "obfuscation was not filtered: {message:?}"
            );
            assert!(
                filtered.contains('*'),
                "obfuscation was changed without being masked: {message:?} -> {filtered:?}"
            );
            assert!(
                filtered.chars().all(|character| character == '*'),
                "part of an obfuscated word survived: {message:?} -> {filtered:?}"
            );
        }
    }

    #[test]
    fn masks_severely_abusive_phrases() {
        let filtered = filter_chat_message("I hope you die");
        assert!(filtered.contains('*'));
        assert!(!filtered.to_lowercase().contains("die"));
    }

    #[test]
    fn preserves_known_false_positives_and_clean_unicode() {
        for message in [
            "assassin",
            "push it",
            "classic",
            "assignment",
            "ho ho ho, Merry Christmas",
            "I'm gay",
            "tit for tat",
            "2 secs",
            "the suicide squad",
            "sperm whale",
            "Scunthorpe is a town",
            "shiitake mushrooms",
            "Middlesex University",
            "Middlesex's campus",
            "Nigeria and Niger",
            "The gaylord hotel",
            "Gaylord Perry was a pitcher",
            "Dickinson College",
            "I hate black coffee",
            "kill the black snake",
            "kill the white snake",
            "kill the male character",
            "kill the female character",
            "kill the disabled process",
            "I hate straight lines",
            "Gay people are allowed",
            "No black snakes allowed",
            "Café mañana — good game!",
            "こんにちは 🐍",
        ] {
            assert_eq!(filter_chat_message(message), message);
        }

        assert_eq!(filter_chat_message("Scunthorpe shit"), "Scunthorpe ****");
    }

    #[test]
    fn preserves_neutral_identity_language_without_exempting_other_content() {
        for message in [
            "gay rights",
            "gay pride",
            "he is gay",
            "I support gay rights",
            "GAY RIGHTS are human rights",
            "lesbian rights",
            "lesbian pride",
            "queer rights",
            "a Jewish person",
            "Jewish",
            "black people",
            "Black people deserve equal rights",
            "We are Homo sapiens",
            "transgender rights",
            "LGBT rights",
            "LGBTQ rights",
            "an LGBT person",
            "LGBTQIA+ players are welcome",
            "ＧＡＹ pride",
            "𝐆𝐀𝐘 pride",
            "ⒼⒶⓎ pride",
        ] {
            assert_eq!(filter_chat_message(message), message);
        }

        assert_eq!(filter_chat_message("gay rights, fuck"), "gay rights, ****");
        assert!(filter_chat_message("black person is a monkey").contains('*'));
        assert!(filter_chat_message("gayass").contains('*'));
        assert!(filter_chat_message("gaz jewish people").contains('*'));
    }

    #[test]
    fn masks_targeted_hate_while_preserving_counter_speech() {
        for message in [
            "I hate gays",
            "gay people are disgusting",
            "kill all gays",
            "black people are monkeys",
            "Jewish vermin",
            "gas all jews",
            "queer freak",
            "lesbian scum",
            "all gay people should die",
            "Muslims are vermin",
            "Muslim people are disgusting",
            "kill all Christians",
            "trans people are disgusting",
            "Latinos are vermin",
            "deport all Latinos",
            "Arabs are disgusting",
            "I hate disabled people",
            "kill all white people",
            "hang all gays",
            "lynch black people",
            "kill all women",
            "kill all immigrants",
            "I despise gays",
            "shoot all gays",
            "burn all Jews",
            "slaughter gay people",
            "attack gay people",
            "sterilize disabled people",
            "beat gay people",
            "ban gay people",
            "kill all Roma",
            "kill all atheists",
            "kill all Palestinians",
            "kill all Chinese people",
            "kill all Israelis",
            "kill all gay players",
            "ban gay users",
            "shoot Muslim gamers",
            "gay players are disgusting",
            "Muslim users are vermin",
            "No gay players allowed",
            "kill some gays",
            "kill many gays",
            "kill several gays",
            "kill most gay people",
            "kill both gay people",
            "kill a few gay people",
            "kill those two gay people",
            "kill those 2 gay people",
            "stab every gay person",
            "execute all gay people",
            "torture gay people",
            "ethnically cleanse Jews",
            "wipe out gays",
            "expel Muslims",
            "purge all Jews",
            "drive out all Muslims",
            "ki-ll all gays",
            "ki_ll all gays",
            "sh o o t all gays",
            "black people are nothing but filthy violent monkeys",
            "black people are nothing but filthy violent savage brutal monkeys",
            "Jewish people are all parasitic vermin",
            "gay people are evil",
            "gays don't belong",
            "gays should perish",
            "Gay people are not equal and are inferior",
            "Gay people are not merely disgusting; they are vermin",
            "Gay people can never stop being disgusting",
            "k1ll all gays",
            "ki\u{200b}ll all gays",
            "kіll all gays",
            "k i l l all gays",
            "k.i.l.l all gays",
            "ｋｉｌｌ all gays",
            "𝐤𝐢𝐥𝐥 all gays",
            "ᴋɪʟʟ all gays",
            "kill every single one of all those gay people",
            "kill every one of those awful horrible gay people",
            "kill every last gay person",
            "kill absolutely all gays",
            "kill all of the local gays",
            "I hate literally every gay person",
            "gay people are dіsgusting",
            "gay people are disg\u{200b}usting",
            "gay people are absolutely disgusting",
            "gay people are truly disgusting",
            "gay people remain disgusting",
            "gay people are the most disgusting",
            "gay people are ｄｉｓｇｕｓｔｉｎｇ",
            "Gay people can never be anything but disgusting",
            "Gay people are monsters",
            "I wish gay people were dead",
            "Death to gay people",
            "No gay people allowed",
            "Do not hesitate, kill all gays",
            "Don't stop, kill gay people",
            "Do not stop them, kill all gays",
            "No one said kill all gays",
            "Why wait, kill all gays",
            "It is wrong to wait; kill all gays",
            "Stop whining, kill all gays",
            "Gay people are not human, they are disgusting",
            "Gay people aren't people, they're vermin",
            "Report users. Kill gays",
            "Protect people. Kill gays",
            "Stop those people! Kill all gays",
            "Don't call me. Gay people are disgusting",
            "It is false that. Gay people are inferior",
            "Say kill all gays",
            "Report users.Kill gays",
            "Report users;Kill gays",
            "Report users, kill gays",
            "Report users—kill gays",
            "Protect people.Kill gays",
            "Don't call me.Gay people are disgusting",
            "kill. all gays",
            "kill; all gays",
            "kill: all gays",
            "kill! all gays",
            "kill\nall gays",
            "shoot; all Muslims",
            "gay people are. disgusting",
            "gays should. die",
            "beat gay players to death",
            "beat all gay players until they die",
            "go beat the gay player to death",
            "beat gay players with bats",
            "beat gay players in real life",
            "No gay players belong here",
            "Report users!Kill gays",
            "Protect people!Kill gays",
            "kill!all gays",
            "kill|all gays",
            "kill@all gays",
            "kill$all gays",
            "Report users．Kill gays",
            "Report users。Kill gays",
            "Report users！Kill gays",
            "Report users/kill gays",
            "physically beat gay players",
            "brutally beat gay players",
            "beat gay players brutally",
            "beat gay players unconscious",
            "beat gay players senseless",
            "beat gay players with fists",
            "beat gay players IRL",
            "beat gay players until they stop moving",
            "beat up gay players",
            "beat gay players to a pulp",
            "kill-gay people",
            "kill/gay people",
            "gay people.disgusting",
            "physically-beat gay players",
            "beat gay players-to death",
            "beat gay players.with fists",
            "beat that gay guy to death",
            "physically beat that gay guy",
            "violently beat that gay guy",
            "beat that gay guy up",
            "beat up that gay guy",
            "beat that gay guy senseless",
            "attack and beat that gay guy",
            "kill all gay people!now",
            "kill all gay people@now",
            "kill all gay people|now",
            "kill all gay people$now",
            "kill all gay players!now",
            "gay people!are disgusting",
            "gay people@are disgusting",
            "gay people|are disgusting",
            "gay people$are disgusting",
            "beat gay players!with bats",
            "beat gay players@with bats",
            "beat gay players|to death",
            "beat gay players$until they die",
            "beat gay players!in real life",
            "gay people: disgusting",
            "gay people, disgusting",
            "Jewish people: vermin",
        ] {
            let filtered = filter_chat_message(message);
            assert!(
                filtered.contains('*'),
                "targeted hate was not masked: {message:?}"
            );
            assert_ne!(filtered, message, "targeted hate survived unchanged");
        }

        assert_eq!(
            filter_chat_message("Gay people are not merely disgusting; they are vermin"),
            "Gay people are not merely **********; they are ******"
        );

        for message in [
            "I hate discrimination against gay people",
            "stop killing gay people",
            "gay people are not disgusting",
            "It is wrong to hate gay people",
            "No one should kill gay people",
            "Nobody should kill gay people",
            "protect gay people from being killed",
            "stop people who kill gay people",
            "Killing gay people is wrong",
            "I don't want to kill gay people",
            "Killing gay people must stop",
            "No one should call Jewish people vermin",
            "Report users who hate gay people",
            "It is false that gay people are inferior",
            "Why do people hate gays?",
            "I hate when gay people are bullied",
            "I don’t hate gay people",
            "The claim that gay people are disgusting is false",
            "Gay people care for monkeys",
            "Jewish cancer research",
            "Black people were murdered in the attack",
            "We must prevent attacks that kill gay people",
            "Do NOT—under any circumstances—kill gay people",
            "He said 'kill all gays' is hateful",
            "We condemn people who call Jewish people vermin",
            "People who call Jewish people vermin are hateful",
            "Kill every snake. Gay people can spectate.",
            "Kill all enemies! Women defend the base.",
            "kill every snake near women",
            "attack every player except gay people",
            "I hate every stereotype about gay people",
            "Ban all slurs about gay people",
            "I hate Chinese food",
            "ban Chinese apps",
            "I hate Palestinian food",
            "I hate lesbian movies",
            "beat the white player",
            "beat the black player",
            "beat the gay player",
            "we can beat the white player",
            "I condemn the phrase kill all gays",
            "Please report threats to kill gay people",
            "It would be terrible to kill gay people",
            "Report anyone who says kill all gays",
            "We cannot allow anyone to kill gay people",
            "Killing gay people is abhorrent",
            "It is wrong to say gay people are disgusting",
            "No, gay people are allowed",
            "Actually no, gay people are allowed",
            "No. Gay people are allowed",
            "No! Gay players allowed",
            "No, gay players belong here",
            "I hate Latino music",
            "I hate Latina music",
            "I hate Hispanic food",
            "Kill. Gay people can spectate.",
            "Attack. Women defend the base.",
            "Shoot! Muslim players joined.",
            "Gay people. Monkeys are nearby.",
            "Women. Monsters are coming.",
            "Gay players. Trash needs cleanup.",
            "we beat gay players. Dead snakes everywhere",
            "we beat gay players. Use bats next round",
            "we beat gay players. In real life I play chess",
            "beat that gay guy at SnakeTron",
            "I beat that gay guy in the game",
            "we beat that gay guy on score",
            "beat that gay guy next round",
            "Gay people. Are monsters nearby?",
            "Women. Are monsters attacking?",
            "Gay people. They are monsters?",
            "Gay people, are monsters nearby?",
            "Gay people: Are monsters nearby?",
            "Gay people — Are monsters nearby?",
            "Gay players, are trash mobs nearby?",
            "we beat gay players, Use bats next round",
            "we beat gay players: In real life I play chess",
            "we beat gay players — Dead snakes everywhere",
        ] {
            assert_eq!(filter_chat_message(message), message);
        }
    }

    #[test]
    fn strips_bidirectional_controls_used_for_visual_evasion() {
        assert_eq!(filter_chat_message("safe\u{202e}text"), "safetext");
        assert_eq!(filter_chat_message("safe\u{2067}text\u{2069}"), "safetext");
        assert_eq!(
            filter_chat_message("か\u{202e}\u{3099} this-shit"),
            "か\u{3099} this-****"
        );
    }

    #[test]
    fn filters_once_with_full_message_context() {
        assert_eq!(
            filter_chat_message("so many a^s hole sin this server"),
            "so many ******** sin this server"
        );
    }

    #[test]
    fn grapheme_mapping_matches_rustricts_normalized_stream() {
        for message in [
            "Cafe\u{301} mañana",
            "か\u{3099} snake",
            "f\u{200b}uck",
            "safe\u{202e}text",
            "か\u{202e}\u{3099} this-shit",
            "こんにちは 🐍",
            "\u{e001}private-use",
        ] {
            let mapped: String = normalize_with_mapping(message)
                .into_iter()
                .map(|character| character.character)
                .collect();
            let mut normalizer = Censor::from_str(message);
            let normalized = normalizer.with_censor_threshold(Type::NONE).censor();
            assert_eq!(mapped, normalized, "mapping drifted for {message:?}");
        }
    }
}
