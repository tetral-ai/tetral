//! Member presence and constant members.
//!
//! The protocol tells an absent member from a `null` one, requires some members
//! to be absent, and fixes others to one value. serde alone treats a missing
//! `Option` member and a `null` one alike, so these types keep the difference.

use std::fmt;

use serde::de::{self, DeserializeOwned};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

/// A member that may be absent, `null` or a value. Declare it with
/// `#[serde(default, skip_serializing_if = "Maybe::is_absent")]`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum Maybe<T> {
    #[default]
    Absent,
    Null,
    Value(T),
}

impl<T> Maybe<T> {
    pub fn is_absent(&self) -> bool {
        matches!(self, Maybe::Absent)
    }

    pub fn value(&self) -> Option<&T> {
        match self {
            Maybe::Value(value) => Some(value),
            _ => None,
        }
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for Maybe<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Option::<T>::deserialize(deserializer).map(|value| value.map_or(Maybe::Null, Maybe::Value))
    }
}

impl<T: Serialize> Serialize for Maybe<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Maybe::Value(value) => value.serialize(serializer),
            Maybe::Absent | Maybe::Null => serializer.serialize_none(),
        }
    }
}

/// A member that must be present and may be `null`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Nullable<T>(pub Option<T>);

impl<'de, T: DeserializeOwned> Deserialize<'de> for Nullable<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        // Through `Value`, because serde reads a missing member as `None` for
        // any type that deserializes through an option.
        match Value::deserialize(deserializer)? {
            Value::Null => Ok(Nullable(None)),
            value => T::deserialize(value)
                .map(|value| Nullable(Some(value)))
                .map_err(de::Error::custom),
        }
    }
}

impl<T: Serialize> Serialize for Nullable<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match &self.0 {
            Some(value) => value.serialize(serializer),
            None => serializer.serialize_none(),
        }
    }
}

/// Reads an optional member that may not be `null`. Declare it with
/// `#[serde(default, deserialize_with = "field::present", skip_serializing_if = "Option::is_none")]`.
pub fn present<'de, D: Deserializer<'de>, T: Deserialize<'de>>(
    deserializer: D,
) -> Result<Option<T>, D::Error> {
    T::deserialize(deserializer).map(Some)
}

/// A member that must be absent: any value, `null` included, is rejected.
/// Declare it with `#[serde(default, skip_serializing)]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Absent;

impl<'de> Deserialize<'de> for Absent {
    fn deserialize<D: Deserializer<'de>>(_: D) -> Result<Self, D::Error> {
        Err(de::Error::custom("a member that must be absent is present"))
    }
}

/// An integer member fixed to `N`, such as `protocol_version` or a
/// `schema_version`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct ConstInt<const N: i64>;

impl<'de, const N: i64> Deserialize<'de> for ConstInt<N> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = i64::deserialize(deserializer)?;
        if value == N {
            Ok(ConstInt)
        } else {
            Err(de::Error::custom(format_args!(
                "expected {N}, found {value}"
            )))
        }
    }
}

impl<const N: i64> Serialize for ConstInt<N> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_i64(N)
    }
}

/// A boolean member fixed to `true`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct True;

impl<'de> Deserialize<'de> for True {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        if bool::deserialize(deserializer)? {
            Ok(True)
        } else {
            Err(de::Error::custom("expected true"))
        }
    }
}

impl Serialize for True {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bool(true)
    }
}

/// A string member fixed to one text, such as the query `protocol_version`
/// `"1"`.
pub trait ConstText {
    const TEXT: &'static str;
}

/// The member type of a [`ConstText`].
pub struct Fixed<T>(std::marker::PhantomData<T>);

// By hand: derived impls would require the marker type itself to implement
// them.
impl<T> Clone for Fixed<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for Fixed<T> {}

impl<T> Default for Fixed<T> {
    fn default() -> Self {
        Fixed(std::marker::PhantomData)
    }
}

impl<T> PartialEq for Fixed<T> {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}

impl<T> Eq for Fixed<T> {}

impl<T: ConstText> fmt::Debug for Fixed<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(T::TEXT, f)
    }
}

impl<'de, T: ConstText> Deserialize<'de> for Fixed<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        if text == T::TEXT {
            Ok(Fixed(std::marker::PhantomData))
        } else {
            Err(de::Error::custom(format_args!("expected {:?}", T::TEXT)))
        }
    }
}

impl<T: ConstText> Serialize for Fixed<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(T::TEXT)
    }
}

/// A string checked against a rule when it is read; the rule is the
/// `TextRule` of the type parameter.
pub trait TextRule {
    const NAME: &'static str;
    fn accepts(text: &str) -> bool;
}

/// A string that passed its [`TextRule`].
pub struct Text<R> {
    text: String,
    rule: std::marker::PhantomData<R>,
}

// By hand, comparing the text alone: derived impls would require the rule
// type itself to implement them.
impl<R> Clone for Text<R> {
    fn clone(&self) -> Self {
        Text {
            text: self.text.clone(),
            rule: std::marker::PhantomData,
        }
    }
}

impl<R> PartialEq for Text<R> {
    fn eq(&self, other: &Self) -> bool {
        self.text == other.text
    }
}

impl<R> Eq for Text<R> {}

impl<R> PartialOrd for Text<R> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<R> Ord for Text<R> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.text.cmp(&other.text)
    }
}

impl<R> std::hash::Hash for Text<R> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.text.hash(state);
    }
}

impl<R: TextRule> Text<R> {
    pub fn new(text: impl Into<String>) -> Result<Self, InvalidText> {
        let text = text.into();
        if R::accepts(&text) {
            Ok(Text {
                text,
                rule: std::marker::PhantomData,
            })
        } else {
            Err(InvalidText(R::NAME))
        }
    }

    pub fn as_str(&self) -> &str {
        &self.text
    }
}

impl<R> fmt::Debug for Text<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.text, f)
    }
}

impl<R> std::ops::Deref for Text<R> {
    type Target = str;
    fn deref(&self) -> &str {
        &self.text
    }
}

impl<'de, R: TextRule> Deserialize<'de> for Text<R> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Text::new(String::deserialize(deserializer)?).map_err(de::Error::custom)
    }
}

impl<R> Serialize for Text<R> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.text)
    }
}

/// A string that its rule refuses; it names the rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("not a valid {0}")]
pub struct InvalidText(pub &'static str);

/// A string with at least one character.
pub enum NonEmpty {}

impl TextRule for NonEmpty {
    const NAME: &'static str = "nonempty string";
    fn accepts(text: &str) -> bool {
        !text.is_empty()
    }
}

/// An absolute path on the machine: it starts with `/` and holds no U+0000.
pub enum Absolute {}

impl TextRule for Absolute {
    const NAME: &'static str = "absolute path";
    fn accepts(text: &str) -> bool {
        text.starts_with('/') && !text.contains('\0')
    }
}

/// A nonempty string without U+0000, the rule of every identity and of the
/// paths that tool inputs name.
pub enum NulFree {}

impl TextRule for NulFree {
    const NAME: &'static str = "nonempty string without U+0000";
    fn accepts(text: &str) -> bool {
        !text.is_empty() && !text.contains('\0')
    }
}

/// A SHA-256 digest in 64 lowercase hexadecimal characters.
pub enum Sha256Hex {}

impl TextRule for Sha256Hex {
    const NAME: &'static str = "SHA-256 in lowercase hexadecimal";
    fn accepts(text: &str) -> bool {
        lower_hex(text, SHA256_HEX_DIGITS)
    }
}

/// A Git commit in 40 lowercase hexadecimal characters.
pub enum CommitHex {}

impl TextRule for CommitHex {
    const NAME: &'static str = "commit SHA in lowercase hexadecimal";
    fn accepts(text: &str) -> bool {
        lower_hex(text, COMMIT_HEX_DIGITS)
    }
}

const SHA256_HEX_DIGITS: usize = 64;
const COMMIT_HEX_DIGITS: usize = 40;

fn lower_hex(text: &str, length: usize) -> bool {
    text.len() == length
        && text
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

pub type NonEmptyString = Text<NonEmpty>;
pub type AbsolutePath = Text<Absolute>;
pub type Sha256 = Text<Sha256Hex>;
pub type CommitSha = Text<CommitHex>;

/// A signed 64-bit integer of at least 1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct PositiveInt(i64);

impl PositiveInt {
    pub fn new(value: i64) -> Option<PositiveInt> {
        (value >= 1).then_some(PositiveInt(value))
    }

    pub fn get(self) -> i64 {
        self.0
    }
}

impl<'de> Deserialize<'de> for PositiveInt {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = i64::deserialize(deserializer)?;
        PositiveInt::new(value).ok_or_else(|| de::Error::custom(format_args!("{value} is below 1")))
    }
}

/// A signed 64-bit integer of at least 0.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct Count(i64);

impl Count {
    pub fn new(value: i64) -> Option<Count> {
        (value >= 0).then_some(Count(value))
    }

    /// `value`, or `i64::MAX` when it is larger.
    pub fn saturating(value: u64) -> Count {
        Count(i64::try_from(value).unwrap_or(i64::MAX))
    }

    pub fn get(self) -> i64 {
        self.0
    }
}

impl<'de> Deserialize<'de> for Count {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = i64::deserialize(deserializer)?;
        Count::new(value).ok_or_else(|| de::Error::custom(format_args!("{value} is below 0")))
    }
}

/// An integer in `MIN..=MAX`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct Bounded<const MIN: i64, const MAX: i64>(i64);

impl<const MIN: i64, const MAX: i64> Bounded<MIN, MAX> {
    pub fn new(value: i64) -> Option<Self> {
        (MIN..=MAX).contains(&value).then_some(Bounded(value))
    }

    pub fn get(self) -> i64 {
        self.0
    }
}

impl<'de, const MIN: i64, const MAX: i64> Deserialize<'de> for Bounded<MIN, MAX> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = i64::deserialize(deserializer)?;
        Bounded::new(value)
            .ok_or_else(|| de::Error::custom(format_args!("{value} is outside {MIN} to {MAX}")))
    }
}

/// An array of `MIN` to `MAX` items.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct List<T, const MIN: usize = 0, const MAX: usize = { usize::MAX }>(pub Vec<T>);

impl<T, const MIN: usize, const MAX: usize> std::ops::Deref for List<T, MIN, MAX> {
    type Target = [T];
    fn deref(&self) -> &[T] {
        &self.0
    }
}

impl<'de, T: Deserialize<'de>, const MIN: usize, const MAX: usize> Deserialize<'de>
    for List<T, MIN, MAX>
{
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let items = Vec::<T>::deserialize(deserializer)?;
        if (MIN..=MAX).contains(&items.len()) {
            Ok(List(items))
        } else {
            Err(de::Error::custom(format_args!(
                "{} items, outside {MIN} to {MAX}",
                items.len()
            )))
        }
    }
}

/// An array of `MIN` to `MAX` distinct items.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct Unique<T, const MIN: usize = 0, const MAX: usize = { usize::MAX }>(pub Vec<T>);

impl<T, const MIN: usize, const MAX: usize> std::ops::Deref for Unique<T, MIN, MAX> {
    type Target = [T];
    fn deref(&self) -> &[T] {
        &self.0
    }
}

impl<'de, T, const MIN: usize, const MAX: usize> Deserialize<'de> for Unique<T, MIN, MAX>
where
    T: Deserialize<'de> + Eq + std::hash::Hash,
{
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let List(items) = List::<T, MIN, MAX>::deserialize(deserializer)?;
        if is_unique(items.iter()) {
            Ok(Unique(items))
        } else {
            Err(de::Error::custom("an item repeats"))
        }
    }
}

/// Whether no two of the items are equal.
pub fn is_unique<T: Eq + std::hash::Hash>(items: impl IntoIterator<Item = T>) -> bool {
    let mut seen = std::collections::HashSet::new();
    items.into_iter().all(|item| seen.insert(item))
}
