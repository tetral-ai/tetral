//! The drive transfer messages: listings of a drive's files and changes,
//! content uploads and downloads with their headers, and commits
//! (`drive-transfer-v1.schema.json`). Query, path and header objects hold the
//! decoded HTTP parameters as strings.
//!
//! A response is read with [`decode_answer`](crate::decode_answer) against
//! the request it answers, so a page that is not after its cursor or a commit
//! result that does not fit its change is refused.

use serde::{Deserialize, Serialize};

use crate::connection::{Downstream, DownstreamPayload, MESSAGE_MAX_BYTES, QueryProtocolVersion};
use crate::field::{
    self, Bounded, ConstText, Fixed, List, NonEmptyString, Nullable, PositiveInt, Sha256, Text,
    TextRule, is_unique,
};
use crate::id::Id;
use crate::path::{DrivePath, case_folded};
use crate::tool::{DriveWriter, ProtocolVersion, RejectionReason};
use crate::{Answer, DecodeError, Message, Received, rule};

/// The limits of `DriveTransferLimits` that the messages below are checked
/// against.
pub mod limits {
    pub const LIST_PAGE_MAX_ITEMS: usize = 1_000;
    pub const LIST_PAGE_MAX_JSON_BYTES: usize = 4_194_304;
    pub const COMMIT_MIN_CHANGES: usize = 1;
    pub const COMMIT_MAX_CHANGES: usize = 10_000;
    pub const COMMIT_REQUEST_MAX_JSON_BYTES: usize = 16_777_216;
    pub const COMMIT_RESPONSE_MAX_JSON_BYTES: usize = 16_777_216;
    pub const RULE_MAX_UTF8_BYTES: usize = 512;
    pub const FILE_MAX_BYTES: i64 = 500_000_000;
    pub const DRIVE_CHANGED_SSE_MAX_JSON_BYTES: usize = 65_536;
    pub const RETRY_AFTER_MIN_SECONDS: u32 = 1;
    pub const RETRY_AFTER_MAX_SECONDS: u32 = 60;
}

pub type FileSize = Bounded<0, { limits::FILE_MAX_BYTES }>;
pub type Seq = PositiveInt;
pub type Index = Bounded<0, { i64::MAX }>;

/// A nonnegative decimal integer within the signed 64-bit range, as a query
/// or header value.
pub type Decimal = Text<DecimalRule>;

pub enum DecimalRule {}

impl TextRule for DecimalRule {
    const NAME: &'static str = "decimal integer";
    fn accepts(text: &str) -> bool {
        decimal(text).is_some()
    }
}

fn decimal(text: &str) -> Option<i64> {
    let canonical = text == "0" || (!text.starts_with('0') && !text.is_empty());
    (canonical && text.bytes().all(|byte| byte.is_ascii_digit()))
        .then(|| text.parse().ok())
        .flatten()
}

/// The value of a [`Decimal`].
pub fn decimal_value(text: &Decimal) -> i64 {
    decimal(text).expect("a decimal holds a signed 64-bit integer")
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct CommonQuery {
    pub protocol_version: QueryProtocolVersion,
    pub connection_id: Id,
    pub binding_id: Id,
}

impl Message for CommonQuery {
    const MAX_BYTES: usize = MESSAGE_MAX_BYTES;
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ListFilesQuery {
    pub protocol_version: QueryProtocolVersion,
    pub connection_id: Id,
    pub binding_id: Id,
    /// The last path of the previous page; absent for the first page.
    #[serde(
        default,
        deserialize_with = "field::present",
        skip_serializing_if = "Option::is_none"
    )]
    pub after_path: Option<String>,
}

impl Message for ListFilesQuery {
    const MAX_BYTES: usize = MESSAGE_MAX_BYTES;
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ListChangesQuery {
    pub protocol_version: QueryProtocolVersion,
    pub connection_id: Id,
    pub binding_id: Id,
    pub after_seq: Decimal,
    /// Present to continue inside the commit `after_seq` names.
    #[serde(
        default,
        deserialize_with = "field::present",
        skip_serializing_if = "Option::is_none"
    )]
    pub after_index: Option<Decimal>,
}

impl Message for ListChangesQuery {
    const MAX_BYTES: usize = MESSAGE_MAX_BYTES;
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DrivePathParams {
    pub drive_id: Id,
}

impl Message for DrivePathParams {
    const MAX_BYTES: usize = MESSAGE_MAX_BYTES;
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ContentPathParams {
    pub drive_id: Id,
    pub sha256: Sha256,
}

impl Message for ContentPathParams {
    const MAX_BYTES: usize = MESSAGE_MAX_BYTES;
}

pub enum OctetStream {}

impl ConstText for OctetStream {
    const TEXT: &'static str = "application/octet-stream";
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct UploadContentRequestHeaders {
    #[serde(rename = "Content-Type")]
    pub content_type: Fixed<OctetStream>,
    /// Required: Engine refuses an upload without it.
    #[serde(rename = "Content-Length")]
    pub content_length: Decimal,
}

impl Message for UploadContentRequestHeaders {
    const MAX_BYTES: usize = MESSAGE_MAX_BYTES;

    fn check(&self, _received: &Received<'_>) -> Result<(), DecodeError> {
        rule(
            decimal_value(&self.content_length) <= limits::FILE_MAX_BYTES,
            "Content-Length over 500,000,000",
        )
    }
}

/// One range of a `Range` header: `first-last`, `first-` or the suffix `-n`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ByteRange {
    From { first: i64, last: Option<i64> },
    Suffix(i64),
}

/// A `Range` value of the form `bytes=<range>[, <range>…]`.
pub type RangeHeader = Text<RangeRule>;

pub enum RangeRule {}

impl TextRule for RangeRule {
    const NAME: &'static str = "byte range";
    fn accepts(text: &str) -> bool {
        byte_ranges(text).is_some()
    }
}

fn byte_ranges(text: &str) -> Option<Vec<ByteRange>> {
    let digits = |part: &str| {
        (!part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit()))
            .then(|| part.parse::<i64>().ok())
            .flatten()
    };
    let specs: Vec<&str> = text.strip_prefix("bytes=")?.split(',').collect();
    let count = specs.len();
    specs
        .into_iter()
        .enumerate()
        .map(|(position, spec)| {
            // Spaces and tabs may stand only next to a comma.
            let blank = [' ', '\t'];
            let spec = if position > 0 {
                spec.trim_start_matches(blank)
            } else {
                spec
            };
            let spec = if position + 1 < count {
                spec.trim_end_matches(blank)
            } else {
                spec
            };
            let (first, last) = spec.split_once('-')?;
            match (first, last) {
                ("", last) => Some(ByteRange::Suffix(digits(last)?)),
                (first, "") => Some(ByteRange::From {
                    first: digits(first)?,
                    last: None,
                }),
                (first, last) => Some(ByteRange::From {
                    first: digits(first)?,
                    last: Some(digits(last)?),
                }),
            }
        })
        .collect()
}

impl RangeRule {
    pub fn ranges(header: &RangeHeader) -> Vec<ByteRange> {
        byte_ranges(header).expect("a range header parses")
    }

    /// The bytes `first..=last` that a 206 answer to `range` over `total`
    /// bytes holds, or `None` where the answer is 416.
    pub fn answer(range: ByteRange, total: i64) -> Option<(i64, i64)> {
        let (first, last) = match range {
            ByteRange::Suffix(length) => (
                (total - length).max(0),
                if length > 0 { total - 1 } else { -1 },
            ),
            ByteRange::From { first, last } => {
                (first, last.map_or(total - 1, |last| last.min(total - 1)))
            }
        };
        (first < total && first <= last).then_some((first, last))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize, Serialize)]
pub struct DownloadContentRequestHeaders {
    #[serde(
        rename = "Range",
        default,
        deserialize_with = "field::present",
        skip_serializing_if = "Option::is_none"
    )]
    pub range: Option<RangeHeader>,
}

impl DownloadContentRequestHeaders {
    /// The one range asked for, or `None` for the whole content.
    pub fn range(&self) -> Option<ByteRange> {
        self.range
            .as_ref()
            .map(|header| RangeRule::ranges(header)[0])
    }
}

impl Message for DownloadContentRequestHeaders {
    const MAX_BYTES: usize = MESSAGE_MAX_BYTES;

    fn check(&self, _received: &Received<'_>) -> Result<(), DecodeError> {
        let Some(header) = &self.range else {
            return Ok(());
        };
        let ranges = RangeRule::ranges(header);
        rule(
            ranges.iter().all(|range| match range {
                ByteRange::From {
                    first,
                    last: Some(last),
                } => first <= last,
                _ => true,
            }),
            "range end before its start",
        )?;
        rule(ranges.len() == 1, "more than one range")
    }
}

/// A `Content-Range` value `bytes <first>-<last>/<total>`.
pub type ContentRange = Text<ContentRangeRule>;

pub enum ContentRangeRule {}

impl TextRule for ContentRangeRule {
    const NAME: &'static str = "content range";
    fn accepts(text: &str) -> bool {
        content_range(text).is_some()
    }
}

fn content_range(text: &str) -> Option<(i64, i64, i64)> {
    let (first, rest) = text.strip_prefix("bytes ")?.split_once('-')?;
    let (last, total) = rest.split_once('/')?;
    Some((decimal(first)?, decimal(last)?, decimal(total)?))
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct PartialContentResponseHeaders {
    #[serde(rename = "Content-Range")]
    pub content_range: ContentRange,
}

impl PartialContentResponseHeaders {
    /// `(first, last, total)`.
    pub fn bounds(&self) -> (i64, i64, i64) {
        content_range(&self.content_range).expect("a content range parses")
    }
}

impl Message for PartialContentResponseHeaders {
    const MAX_BYTES: usize = MESSAGE_MAX_BYTES;

    fn check(&self, _received: &Received<'_>) -> Result<(), DecodeError> {
        let (first, last, total) = self.bounds();
        rule(
            first <= last && last < total && total <= limits::FILE_MAX_BYTES,
            "content range outside the content",
        )
    }
}

impl Answer<DownloadContentRequestHeaders> for PartialContentResponseHeaders {
    fn check_answer(&self, request: &DownloadContentRequestHeaders) -> Result<(), DecodeError> {
        let (first, last, total) = self.bounds();
        let asked = request
            .range()
            .and_then(|range| RangeRule::answer(range, total));
        rule(
            asked == Some((first, last)),
            "content range not the one requested",
        )
    }
}

/// A `Retry-After` value: whole seconds from 1 to 60.
pub type RetryAfter = Text<RetryAfterRule>;

pub enum RetryAfterRule {}

impl TextRule for RetryAfterRule {
    const NAME: &'static str = "Retry-After seconds from 1 to 60";
    fn accepts(text: &str) -> bool {
        decimal(text).is_some_and(|seconds| {
            (i64::from(limits::RETRY_AFTER_MIN_SECONDS)
                ..=i64::from(limits::RETRY_AFTER_MAX_SECONDS))
                .contains(&seconds)
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct RateLimitedResponseHeaders {
    #[serde(rename = "Retry-After")]
    pub retry_after: RetryAfter,
}

impl Message for RateLimitedResponseHeaders {
    const MAX_BYTES: usize = MESSAGE_MAX_BYTES;
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct FileEntry {
    pub path: DrivePath,
    pub version_id: Id,
    pub content_sha256: Sha256,
    pub size_bytes: FileSize,
    pub executable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ListFilesResponse {
    pub files: List<FileEntry, 0, { limits::LIST_PAGE_MAX_ITEMS }>,
    pub has_more: bool,
    /// The drive's change sequence at the first page, from which catch-up
    /// lists changes; only the first page carries it.
    #[serde(
        default,
        deserialize_with = "field::present",
        skip_serializing_if = "Option::is_none"
    )]
    pub start_seq: Option<Bounded<0, { i64::MAX }>>,
}

impl Message for ListFilesResponse {
    const MAX_BYTES: usize = limits::LIST_PAGE_MAX_JSON_BYTES;

    fn check(&self, _received: &Received<'_>) -> Result<(), DecodeError> {
        let paths: Vec<&str> = self.files.iter().map(|entry| entry.path.as_str()).collect();
        // Strictly increasing, so no path is listed twice.
        rule(
            paths
                .windows(2)
                .all(|pair| pair[0].as_bytes() < pair[1].as_bytes()),
            "files not in path order",
        )?;
        rule(
            is_unique(paths.iter().map(|path| case_folded(path))),
            "paths that differ only in case",
        )?;
        rule(
            paths.iter().all(|path| {
                path.match_indices('/').all(|(slash, _)| {
                    paths
                        .binary_search_by(|other| other.as_bytes().cmp(&path.as_bytes()[..slash]))
                        .is_err()
                })
            }),
            "a file listed above another file",
        )?;
        rule(
            !self.has_more || !paths.is_empty(),
            "has_more on an empty page",
        )
    }
}

impl Answer<ListFilesQuery> for ListFilesResponse {
    fn check_answer(&self, query: &ListFilesQuery) -> Result<(), DecodeError> {
        rule(
            self.start_seq.is_some() == query.after_path.is_none(),
            "start_seq exactly on the first page",
        )?;
        match (&query.after_path, self.files.first()) {
            (Some(after), Some(first)) => rule(
                first.path.as_bytes() > after.as_bytes(),
                "page not after its cursor",
            ),
            _ => Ok(()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Operation {
    Created,
    Modified,
    Deleted,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ChangeRecord {
    pub seq: Seq,
    pub index: Index,
    pub path: DrivePath,
    pub operation: Operation,
    pub version_id: Id,
    /// `None` exactly for a deletion.
    pub content_sha256: Nullable<Sha256>,
    pub size_bytes: FileSize,
    pub executable: bool,
}

impl ChangeRecord {
    fn check(&self) -> Result<(), DecodeError> {
        if self.operation == Operation::Deleted {
            rule(
                self.content_sha256.0.is_none() && self.size_bytes.get() == 0 && !self.executable,
                "a deletion with content",
            )
        } else {
            rule(self.content_sha256.0.is_some(), "a write without content")
        }
    }

    fn key(&self) -> (i64, i64) {
        (self.seq.get(), self.index.get())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ListChangesResponse {
    pub records: List<ChangeRecord, 0, { limits::LIST_PAGE_MAX_ITEMS }>,
    pub has_more: bool,
}

impl Message for ListChangesResponse {
    const MAX_BYTES: usize = limits::LIST_PAGE_MAX_JSON_BYTES;

    fn check(&self, _received: &Received<'_>) -> Result<(), DecodeError> {
        for record in self.records.iter() {
            record.check()?;
        }
        // Within one commit, records count up from index 0, deletions first.
        for pair in self.records.windows(2) {
            let (earlier, later) = (&pair[0], &pair[1]);
            rule(earlier.key() < later.key(), "records not in order")?;
            if earlier.seq == later.seq {
                rule(later.index.get() == earlier.index.get() + 1, "index gap")?;
                rule(
                    earlier.operation == Operation::Deleted
                        || later.operation != Operation::Deleted,
                    "a deletion after a write",
                )?;
            } else {
                rule(later.index.get() == 0, "index gap")?;
            }
        }
        rule(
            !self.has_more || !self.records.is_empty(),
            "has_more on an empty page",
        )
    }
}

impl Answer<ListChangesQuery> for ListChangesResponse {
    fn check_answer(&self, query: &ListChangesQuery) -> Result<(), DecodeError> {
        let Some(first) = self.records.first() else {
            return Ok(());
        };
        let seq = decimal_value(&query.after_seq);
        match &query.after_index {
            Some(index) => {
                let index = decimal_value(index);
                rule(first.key() > (seq, index), "page not after its cursor")?;
                let next = if first.seq.get() == seq { index + 1 } else { 0 };
                rule(first.index.get() == next, "index gap")
            }
            None => {
                rule(first.seq.get() > seq, "page not after its cursor")?;
                rule(first.index.get() == 0, "index gap")
            }
        }
    }
}

/// A `drive.changed` event of the connection's stream.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(try_from = "Downstream", into = "Downstream")]
pub struct DriveChanged(pub Downstream);

impl TryFrom<Downstream> for DriveChanged {
    type Error = String;

    fn try_from(downstream: Downstream) -> Result<Self, String> {
        match downstream.payload {
            DownstreamPayload::DriveChanged(_) => Ok(DriveChanged(downstream)),
            _ => Err("not a drive.changed event".to_owned()),
        }
    }
}

impl From<DriveChanged> for Downstream {
    fn from(event: DriveChanged) -> Self {
        event.0
    }
}

impl Message for DriveChanged {
    const MAX_BYTES: usize = limits::DRIVE_CHANGED_SSE_MAX_JSON_BYTES;

    fn check(&self, received: &Received<'_>) -> Result<(), DecodeError> {
        self.0.check(received)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct UploadContentResponse {
    pub content_sha256: Sha256,
    pub size_bytes: FileSize,
}

impl Message for UploadContentResponse {
    const MAX_BYTES: usize = MESSAGE_MAX_BYTES;
}

/// What an upload response answers: the content's URL and its headers.
pub struct UploadRequest<'a> {
    pub path: &'a ContentPathParams,
    pub headers: &'a UploadContentRequestHeaders,
}

impl Answer<UploadRequest<'_>> for UploadContentResponse {
    fn check_answer(&self, request: &UploadRequest<'_>) -> Result<(), DecodeError> {
        rule(
            self.content_sha256 == request.path.sha256,
            "hash differs from the URL",
        )?;
        rule(
            self.size_bytes.get() == decimal_value(&request.headers.content_length),
            "size differs from Content-Length",
        )
    }
}

/// One change of a commit. A closed object: a member the hub does not know,
/// such as a writer, is refused rather than ignored.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "change", rename_all = "snake_case", deny_unknown_fields)]
pub enum CommitChange {
    Write {
        /// Any string; the hub refuses a path that breaks the path rules in
        /// that change's result.
        path: String,
        /// The content this machine last saw, `None` for a new file.
        base_sha256: Nullable<Sha256>,
        content_sha256: Sha256,
        executable: bool,
    },
    Delete {
        path: String,
        base_sha256: Sha256,
    },
}

impl CommitChange {
    pub fn path(&self) -> &str {
        match self {
            CommitChange::Write { path, .. } | CommitChange::Delete { path, .. } => path,
        }
    }

    fn base(&self) -> Option<&Sha256> {
        match self {
            CommitChange::Write { base_sha256, .. } => base_sha256.0.as_ref(),
            CommitChange::Delete { base_sha256, .. } => Some(base_sha256),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CommitRequest {
    pub protocol_version: ProtocolVersion,
    pub connection_id: Id,
    pub binding_id: Id,
    pub changes: List<CommitChange, { limits::COMMIT_MIN_CHANGES }, { limits::COMMIT_MAX_CHANGES }>,
}

impl Message for CommitRequest {
    const MAX_BYTES: usize = limits::COMMIT_REQUEST_MAX_JSON_BYTES;

    fn check(&self, _received: &Received<'_>) -> Result<(), DecodeError> {
        rule(
            is_unique(self.changes.iter().map(CommitChange::path)),
            "path twice in one commit",
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct CurrentVersion {
    pub version_id: Id,
    /// `None` when the path's current version is a deletion.
    pub content_sha256: Nullable<Sha256>,
    pub size_bytes: FileSize,
    pub executable: bool,
    pub writer: DriveWriter,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum CommitResult {
    Accepted {
        /// `None` only for the deletion of a path that holds no file.
        version_id: Nullable<Id>,
    },
    Rejected {
        reason: RejectionReason,
        /// The rule broken, for `path_rule` and `limit`.
        #[serde(
            default,
            deserialize_with = "field::present",
            skip_serializing_if = "Option::is_none"
        )]
        rule: Option<NonEmptyString>,
        #[serde(
            default,
            deserialize_with = "field::present",
            skip_serializing_if = "Option::is_none"
        )]
        current: Option<CurrentVersion>,
    },
}

impl CommitResult {
    fn check(&self) -> Result<(), DecodeError> {
        let CommitResult::Rejected {
            reason,
            rule: broken,
            current,
        } = self
        else {
            return Ok(());
        };
        let needs_rule = matches!(reason, RejectionReason::PathRule | RejectionReason::Limit);
        rule(
            !needs_rule || broken.is_some(),
            "path_rule or limit without its rule",
        )?;
        rule(
            broken
                .as_ref()
                .is_none_or(|text| text.len() <= limits::RULE_MAX_UTF8_BYTES),
            "rule over 512 bytes",
        )?;
        rule(
            current.as_ref().is_none_or(|current| {
                current.content_sha256.0.is_some()
                    || (current.size_bytes.get() == 0 && !current.executable)
            }),
            "a current deletion with content",
        )
    }

    /// The relations between a change, its result and the path's current
    /// version: a rejection says why the hub did not take a change it could
    /// not have taken, in the order the hub decides.
    fn check_decision(&self, change: &CommitChange) -> Result<(), DecodeError> {
        let (reason, current) = match self {
            CommitResult::Accepted { version_id } => {
                let write = matches!(change, CommitChange::Write { .. });
                return rule(
                    !write || version_id.0.is_some(),
                    "an accepted write without a version",
                );
            }
            CommitResult::Rejected {
                reason, current, ..
            } => (*reason, current.as_ref()),
        };
        let held = current.and_then(|current| current.content_sha256.0.as_ref());
        match change {
            CommitChange::Write {
                content_sha256,
                executable,
                ..
            } => {
                let holds = current.is_some_and(|current| {
                    held == Some(content_sha256) && current.executable == *executable
                });
                rule(!holds, "a write rejected where the drive already holds it")?;
            }
            CommitChange::Delete { .. } => rule(
                held.is_some() && reason != RejectionReason::Limit,
                "a deletion rejected where it could not have failed",
            )?,
        }
        match reason {
            RejectionReason::Conflict => {
                rule(held != change.base(), "a conflict on a matching base")
            }
            RejectionReason::Limit => rule(held == change.base(), "a limit on a conflicting base"),
            _ => Ok(()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct CommitResponse {
    /// The drive's new change sequence, `None` when the commit made no new
    /// version.
    pub seq: Nullable<Seq>,
    pub results: List<CommitResult, { limits::COMMIT_MIN_CHANGES }, { limits::COMMIT_MAX_CHANGES }>,
}

impl Message for CommitResponse {
    const MAX_BYTES: usize = limits::COMMIT_RESPONSE_MAX_JSON_BYTES;

    fn check(&self, _received: &Received<'_>) -> Result<(), DecodeError> {
        for result in self.results.iter() {
            result.check()?;
        }
        rule(
            self.seq.0.is_none()
                || self
                    .results
                    .iter()
                    .any(|result| matches!(result, CommitResult::Accepted { .. })),
            "seq without a new version",
        )
    }
}

impl Answer<CommitRequest> for CommitResponse {
    fn check_answer(&self, request: &CommitRequest) -> Result<(), DecodeError> {
        rule(
            self.results.len() == request.changes.len(),
            "result count differs from change count",
        )?;
        for (change, result) in request.changes.iter().zip(self.results.iter()) {
            result.check_decision(change)?;
        }
        Ok(())
    }
}
