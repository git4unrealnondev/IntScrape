use nohash_hasher::IntMap;
use shared_types::TagSearch;
use std::cmp::Ordering;
use std::collections::HashSet;
use strsim::levenshtein;

pub(crate) const POPULAR_TAG_CACHE_LIMIT: usize = 100_000;
pub(crate) const FTS_CANDIDATE_LIMIT: usize = 4096;

#[derive(Clone)]
pub(crate) struct TagEntry {
    pub(crate) tag_id: u64,
    pub(crate) normalized_name: String,
    pub(crate) count: u64,
}

struct CompactTagEntry {
    tag_id: u64,
    name_start: usize,
    name_len: usize,
    name_char_len: usize,
    count: u64,
}

#[derive(Default)]
pub(crate) struct TagSearchCache {
    entries: Vec<CompactTagEntry>,
    names: Vec<u8>,
    prefix_buckets: IntMap<u32, Vec<usize>>,
    gram_offsets: Vec<usize>,
    gram_postings: Vec<usize>,
    complete: bool,
}

impl TagSearchCache {
    pub(crate) fn from_entries(entries: Vec<TagEntry>) -> Self {
        Self::from_entries_with_completeness(entries, false)
    }

    pub(crate) fn from_entries_with_completeness(entries: Vec<TagEntry>, complete: bool) -> Self {
        let mut compact_entries = Vec::with_capacity(entries.len());
        let mut names = Vec::new();
        for entry in entries {
            let name_start = names.len();
            names.extend_from_slice(entry.normalized_name.as_bytes());
            compact_entries.push(CompactTagEntry {
                tag_id: entry.tag_id,
                name_start,
                name_len: entry.normalized_name.len(),
                name_char_len: entry.normalized_name.chars().count(),
                count: entry.count,
            });
        }
        let mut prefix_buckets: IntMap<u32, Vec<usize>> = IntMap::default();
        // Two-byte postings narrow fuzzy searches to tags sharing query material.
        let mut gram_counts = vec![0usize; u16::MAX as usize + 1];
        let mut seen_grams = vec![0u32; u16::MAX as usize + 1];
        for (entry_index, entry) in compact_entries.iter().enumerate() {
            let name = name_from_parts(&names, entry).as_bytes();
            if name.len() >= 3 {
                prefix_buckets
                    .entry(prefix_key(name))
                    .or_default()
                    .push(entry_index);
            }
            for gram in name.windows(2) {
                let key = u16::from_be_bytes([gram[0], gram[1]]) as usize;
                if seen_grams[key] != entry_index as u32 + 1 {
                    seen_grams[key] = entry_index as u32 + 1;
                    gram_counts[key] += 1;
                }
            }
        }
        // Build a compact CSR-style index: offsets identify each gram's slice
        // in one contiguous posting array.
        let mut gram_offsets = vec![0usize; gram_counts.len() + 1];
        for index in 0..gram_counts.len() {
            gram_offsets[index + 1] = gram_offsets[index] + gram_counts[index];
        }
        let mut gram_postings = vec![0usize; *gram_offsets.last().unwrap_or(&0)];
        let mut gram_positions = gram_offsets[..gram_counts.len()].to_vec();
        seen_grams.fill(0);
        for (entry_index, entry) in compact_entries.iter().enumerate() {
            let name = name_from_parts(&names, entry).as_bytes();
            for gram in name.windows(2) {
                let key = u16::from_be_bytes([gram[0], gram[1]]) as usize;
                if seen_grams[key] != entry_index as u32 + 1 {
                    seen_grams[key] = entry_index as u32 + 1;
                    let position = gram_positions[key];
                    gram_postings[position] = entry_index;
                    gram_positions[key] += 1;
                }
            }
        }
        Self {
            entries: compact_entries,
            names,
            prefix_buckets,
            gram_offsets,
            gram_postings,
            complete,
        }
    }

    pub(crate) fn is_complete(&self) -> bool {
        self.complete
    }

    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(crate) fn search(&self, query: &str, limit: usize) -> Vec<TagSearch> {
        let normalized_query = normalize(query);
        if normalized_query.is_empty() || limit == 0 {
            return Vec::new();
        }

        let query_char_len = normalized_query.chars().count();
        let query_bytes = normalized_query.as_bytes();
        let mut prefix_matches: Vec<&CompactTagEntry> = Vec::with_capacity(limit);
        let prefix_candidates = query_bytes
            .get(..3)
            .map(|key| prefix_key(key))
            .and_then(|key| self.prefix_buckets.get(&key));
        for &entry_index in prefix_candidates.into_iter().flatten() {
            let entry = &self.entries[entry_index];
            let name = name_from_parts(&self.names, entry);
            if !name.starts_with(&normalized_query) {
                continue;
            }
            if prefix_matches.len() < limit {
                prefix_matches.push(entry);
            } else {
                let mut worst_index = 0;
                for (index, candidate) in prefix_matches.iter().enumerate().skip(1) {
                    if prefix_is_worse(candidate, prefix_matches[worst_index]) {
                        worst_index = index;
                    }
                }
                if prefix_is_better(entry, prefix_matches[worst_index]) {
                    prefix_matches[worst_index] = entry;
                }
            }
        }
        if prefix_matches.len() >= limit {
            prefix_matches.sort_unstable_by(|left, right| {
                right
                    .count
                    .cmp(&left.count)
                    .then_with(|| left.tag_id.cmp(&right.tag_id))
            });
            return prefix_matches
                .into_iter()
                .take(limit)
                .map(|entry| TagSearch {
                    tag_id: entry.tag_id,
                    count: entry.count,
                })
                .collect();
        }

        let mut matches: Vec<(&CompactTagEntry, usize)> = Vec::with_capacity(limit);
        let ascii_query = normalized_query.is_ascii();
        let query_bytes = normalized_query.as_bytes();
        let mut distance_workspace = vec![0; 2 * (query_bytes.len() + 1)];
        let mut candidate_hits = None;
        let mut fuzzy_threshold = 0u8;
        let candidate_indices = if ascii_query
            && query_bytes.len() > 2 * allowed_distance(query_bytes.len())
        {
            let mut candidate_marks = vec![false; self.entries.len()];
            let mut hits = vec![0u8; self.entries.len()];
            let mut candidates = Vec::new();
            let gram_count = query_bytes.len() - 1;
            let mut stack_grams = [0usize; 256];
            let mut heap_grams = Vec::new();
            let grams: &mut [usize] = if gram_count <= stack_grams.len() {
                for (index, gram) in query_bytes.windows(2).enumerate() {
                    stack_grams[index] = u16::from_be_bytes([gram[0], gram[1]]) as usize;
                }
                &mut stack_grams[..gram_count]
            } else {
                heap_grams.extend(
                    query_bytes
                        .windows(2)
                        .map(|gram| u16::from_be_bytes([gram[0], gram[1]]) as usize),
                );
                heap_grams.as_mut_slice()
            };
            grams.sort_unstable_by_key(|&key| self.gram_offsets[key + 1] - self.gram_offsets[key]);
            let mut unique_count = 0;
            for index in 0..grams.len() {
                if unique_count == 0 || grams[index] != grams[unique_count - 1] {
                    grams.swap(unique_count, index);
                    unique_count += 1;
                }
            }
            for &key in &grams[..unique_count] {
                for &entry_index in
                    &self.gram_postings[self.gram_offsets[key]..self.gram_offsets[key + 1]]
                {
                    if !candidate_marks[entry_index] {
                        candidate_marks[entry_index] = true;
                        candidates.push(entry_index);
                    }
                    hits[entry_index] = hits[entry_index].saturating_add(1);
                }
            }
            fuzzy_threshold = query_bytes
                .len()
                .saturating_sub(1 + 2 * allowed_distance(query_bytes.len()))
                .max(1) as u8;
            candidate_hits = Some(hits);
            Some(candidates)
        } else {
            None
        };

        if let Some(candidate_indices) = candidate_indices {
            for entry_index in candidate_indices {
                let entry = &self.entries[entry_index];
                add_candidate_match(
                    entry_index,
                    entry,
                    &normalized_query,
                    query_char_len,
                    ascii_query,
                    query_bytes,
                    &self.names,
                    &mut distance_workspace,
                    &mut matches,
                    limit,
                    candidate_hits.as_deref(),
                    fuzzy_threshold,
                );
            }
        } else {
            for (entry_index, entry) in self.entries.iter().enumerate() {
                add_candidate_match(
                    entry_index,
                    entry,
                    &normalized_query,
                    query_char_len,
                    ascii_query,
                    query_bytes,
                    &self.names,
                    &mut distance_workspace,
                    &mut matches,
                    limit,
                    None,
                    0,
                );
            }
        }

        // Sort only the final `limit` elements (max 50 items, extremely fast)
        matches.sort_unstable_by(|(left, left_score), (right, right_score)| {
            left_score
                .cmp(right_score)
                .then_with(|| right.count.cmp(&left.count))
                .then_with(|| left.tag_id.cmp(&right.tag_id))
        });

        matches
            .into_iter()
            .map(|(entry, _)| TagSearch {
                tag_id: entry.tag_id,
                count: entry.count,
            })
            .collect()
    }
}

fn add_candidate_match<'a>(
    entry_index: usize,
    entry: &'a CompactTagEntry,
    normalized_query: &str,
    query_char_len: usize,
    ascii_query: bool,
    query_bytes: &[u8],
    names: &[u8],
    distance_workspace: &mut [usize],
    matches: &mut Vec<(&'a CompactTagEntry, usize)>,
    limit: usize,
    candidate_hits: Option<&[u8]>,
    fuzzy_threshold: u8,
) {
    if entry.name_len < normalized_query.len() {
        return;
    }
    let name = name_from_parts(names, entry);
    if let Some(hits) = candidate_hits {
        if hits[entry_index] < fuzzy_threshold {
            return;
        }
    }
    let score = if ascii_query && name.is_ascii() {
        match_score_ascii(query_bytes, name.as_bytes(), distance_workspace)
    } else {
        match_score_with_lengths(normalized_query, name, query_char_len, entry.name_char_len)
    };
    let Some(score) = score else {
        return;
    };
    if matches.len() < limit {
        matches.push((entry, score));
        return;
    }
    let mut worst_index = 0;
    for (index, candidate) in matches.iter().enumerate().skip(1) {
        if is_worse(*candidate, matches[worst_index]) {
            worst_index = index;
        }
    }
    if is_better(score, entry, matches[worst_index]) {
        matches[worst_index] = (entry, score);
    }
}

fn prefix_is_worse(left: &CompactTagEntry, right: &CompactTagEntry) -> bool {
    left.count < right.count || (left.count == right.count && left.tag_id > right.tag_id)
}

fn prefix_is_better(left: &CompactTagEntry, right: &CompactTagEntry) -> bool {
    left.count > right.count || (left.count == right.count && left.tag_id < right.tag_id)
}

fn prefix_key(bytes: &[u8]) -> u32 {
    u32::from_be_bytes([0, bytes[0], bytes[1], bytes[2]])
}

fn match_score_ascii(query: &[u8], candidate: &[u8], workspace: &mut [usize]) -> Option<usize> {
    if candidate.starts_with(query) {
        return Some(0);
    }
    if candidate.windows(query.len()).any(|window| window == query) {
        return Some(1);
    }

    let max_distance = allowed_distance(query.len());
    if query.len().abs_diff(candidate.len()) > max_distance {
        return None;
    }

    let (previous, current) = workspace.split_at_mut(query.len() + 1);
    for (index, value) in previous.iter_mut().enumerate() {
        *value = index;
    }
    for (candidate_index, candidate_byte) in candidate.iter().enumerate() {
        current[0] = candidate_index + 1;
        for query_index in 1..=query.len() {
            current[query_index] = (previous[query_index] + 1)
                .min(current[query_index - 1] + 1)
                .min(
                    previous[query_index - 1]
                        + usize::from(query[query_index - 1] != *candidate_byte),
                );
        }
        if current.iter().copied().min().unwrap_or(0) > max_distance {
            return None;
        }
        previous.copy_from_slice(current);
    }

    (workspace[query.len()] <= max_distance).then_some(workspace[query.len()] + 2)
}

fn name_from_parts<'a>(names: &'a [u8], entry: &CompactTagEntry) -> &'a str {
    let Some(end) = entry.name_start.checked_add(entry.name_len) else {
        return "";
    };
    let Some(bytes) = names.get(entry.name_start..end) else {
        return "";
    };
    // Names are normalized before entering the compact cache and are valid UTF-8.
    unsafe { std::str::from_utf8_unchecked(bytes) }
}

fn is_worse(left: (&CompactTagEntry, usize), right: (&CompactTagEntry, usize)) -> bool {
    left.1 > right.1
        || (left.1 == right.1
            && (left.0.count < right.0.count
                || (left.0.count == right.0.count && left.0.tag_id > right.0.tag_id)))
}

fn is_better(score: usize, entry: &CompactTagEntry, worst: (&CompactTagEntry, usize)) -> bool {
    score < worst.1
        || (score == worst.1
            && (entry.count > worst.0.count
                || (entry.count == worst.0.count && entry.tag_id < worst.0.tag_id)))
}

/// Searches a streamed set of entries without materializing the full set.
pub(crate) fn search_entries<I>(
    entries: I,
    query: &str,
    limit: usize,
    excluded_ids: &HashSet<u64>,
) -> Vec<TagSearch>
where
    I: IntoIterator<Item = TagEntry>,
{
    let normalized_query = normalize(query);
    if normalized_query.is_empty() || limit == 0 {
        return Vec::new();
    }

    let mut matches: Vec<(TagEntry, usize)> = Vec::with_capacity(limit);
    for entry in entries {
        if excluded_ids.contains(&entry.tag_id) {
            continue;
        }

        let Some(score) = match_score_with_lengths(
            &normalized_query,
            &entry.normalized_name,
            normalized_query.chars().count(),
            entry.normalized_name.chars().count(),
        ) else {
            continue;
        };

        if matches.len() < limit {
            matches.push((entry, score));
            continue;
        }

        let mut worst_index = 0;
        for index in 1..matches.len() {
            let (_, candidate_score) = &matches[index];
            let (_, worst_score) = &matches[worst_index];
            if candidate_score > worst_score
                || (candidate_score == worst_score
                    && (matches[index].0.count < matches[worst_index].0.count
                        || (matches[index].0.count == matches[worst_index].0.count
                            && matches[index].0.tag_id > matches[worst_index].0.tag_id)))
            {
                worst_index = index;
            }
        }

        let (_, worst_score) = &matches[worst_index];
        let is_better = score < *worst_score
            || (score == *worst_score
                && (entry.count > matches[worst_index].0.count
                    || (entry.count == matches[worst_index].0.count
                        && entry.tag_id < matches[worst_index].0.tag_id)));
        if is_better {
            matches[worst_index] = (entry, score);
        }
    }

    matches.sort_unstable_by(|(left, left_score), (right, right_score)| {
        left_score
            .cmp(right_score)
            .then_with(|| right.count.cmp(&left.count))
            .then_with(|| left.tag_id.cmp(&right.tag_id))
    });
    matches
        .into_iter()
        .map(|(entry, _)| TagSearch {
            tag_id: entry.tag_id,
            count: entry.count,
        })
        .collect()
}

pub(crate) fn tag_entry(tag_id: u64, name: &str, count: u64) -> TagEntry {
    TagEntry {
        tag_id,
        normalized_name: normalize(name),
        count,
    }
}

pub(crate) fn normalize(value: &str) -> String {
    value
        .chars()
        .filter(|character| character.is_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect()
}

pub(crate) fn fts_query(value: &str) -> Option<String> {
    let mut terms = HashSet::new();
    let normalized = normalize(value);
    if normalized.chars().count() >= 3 {
        terms.insert(format!("\"{normalized}\""));
    }
    for term in value
        .split(|character: char| !character.is_alphanumeric())
        .map(normalize)
        .filter(|term| term.chars().count() >= 3)
    {
        terms.insert(format!("\"{term}\""));
        let characters: Vec<char> = term.chars().collect();
        for index in 0..characters.len().saturating_sub(1) {
            let mut swapped = characters.clone();
            swapped.swap(index, index + 1);
            terms.insert(format!("\"{}\"", swapped.into_iter().collect::<String>()));
        }
    }
    (!terms.is_empty()).then(|| {
        let mut terms: Vec<_> = terms.into_iter().collect();
        terms.sort_unstable();
        terms.join(" OR ")
    })
}

fn allowed_distance(length: usize) -> usize {
    match length {
        0..=4 => 1,
        5..=8 => 2,
        _ => 3,
    }
}

fn match_score(query: &str, candidate: &str) -> Option<usize> {
    match_score_with_lengths(
        query,
        candidate,
        query.chars().count(),
        candidate.chars().count(),
    )
}

fn match_score_with_lengths(
    query: &str,
    candidate: &str,
    query_len: usize,
    candidate_len: usize,
) -> Option<usize> {
    if candidate.starts_with(query) {
        return Some(0);
    }
    if candidate.contains(query) {
        return Some(1);
    }

    let max_distance = allowed_distance(query.len());
    if query_len.abs_diff(candidate_len) > max_distance {
        return None;
    }
    let distance = levenshtein(query, candidate);
    (distance <= max_distance).then_some(distance + 2)
}

pub(crate) fn compare_results(left: &TagSearch, right: &TagSearch) -> Ordering {
    right
        .count
        .cmp(&left.count)
        .then_with(|| left.tag_id.cmp(&right.tag_id))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn streamed_search_keeps_only_requested_result_count() {
        let entries = (0..10_000).map(|id| tag_entry(id, &format!("common-tag-{id}"), id));

        let results = search_entries(entries, "common", 3, &HashSet::new());

        assert_eq!(results.len(), 3);
        assert_eq!(results[0].tag_id, 9999);
    }

    #[test]
    fn compact_cache_ignores_invalid_name_ranges() {
        let cache = TagSearchCache {
            entries: vec![CompactTagEntry {
                tag_id: 42,
                name_start: usize::MAX,
                name_len: 312,
                name_char_len: 312,
                count: 1,
            }],
            names: b"valid".to_vec(),
            prefix_buckets: IntMap::default(),
            gram_offsets: vec![0; u16::MAX as usize + 2],
            gram_postings: vec![],
            complete: false,
        };

        assert!(cache.search("valid", 1).is_empty());
    }

    #[test]
    fn fts_query_deduplicates_typo_candidates() {
        let query = fts_query("fmaale").unwrap();
        assert_eq!(query.matches("\"fmaale\"").count(), 1);
    }

    #[test]
    fn cache_tracks_when_it_contains_the_whole_tag_table() {
        let complete = TagSearchCache::from_entries_with_completeness(vec![], true);
        let partial = TagSearchCache::from_entries_with_completeness(vec![], false);

        assert!(complete.is_complete());
        assert!(!partial.is_complete());
    }

    #[test]
    fn length_rejection_preserves_fuzzy_match_behavior() {
        assert_eq!(match_score("kitten", "sitten"), Some(3));
        assert_eq!(match_score("kitten", "xxxxxx-long-name"), None);
        assert_eq!(match_score("猫", "猫"), Some(0));
        assert_eq!(match_score("猫", "犬犬犬"), None);
    }

    #[test]
    fn prefix_and_substring_matches_keep_their_scores() {
        assert_eq!(match_score("fem", "female"), Some(0));
        assert_eq!(match_score("male", "female"), Some(1));
        assert_eq!(match_score("猫", "黒猫"), Some(1));
    }

    #[test]
    fn prefix_fast_path_keeps_count_and_id_ordering() {
        let cache = TagSearchCache::from_entries_with_completeness(
            vec![
                tag_entry(1, "female", 2),
                tag_entry(2, "females", 10),
                tag_entry(3, "feminine", 5),
                tag_entry(4, "other", 100),
            ],
            true,
        );

        let results = cache.search("fem", 2);
        assert_eq!(
            results
                .iter()
                .map(|result| result.tag_id)
                .collect::<Vec<_>>(),
            vec![2, 3]
        );
    }

    #[test]
    fn real_general_namespace_words_match_expected_prefixes() {
        let cache = TagSearchCache::from_entries_with_completeness(
            vec![
                tag_entry(383, "female", 78),
                tag_entry(282, "male", 74),
                tag_entry(271, "anthro", 64),
                tag_entry(274, "duo", 64),
                tag_entry(382, "breasts", 53),
                tag_entry(330, "clothing", 42),
                tag_entry(341, "hair", 28),
                tag_entry(853, "white_hair", 15),
            ],
            true,
        );

        assert_eq!(cache.search("fem", 10)[0].tag_id, 383);
        assert_eq!(cache.search("breast", 10)[0].tag_id, 382);
        assert_eq!(cache.search("white hair", 10)[0].tag_id, 853);
    }
}
