//! The media of a call's result: every `data` of an `image` or `audio`
//! block and every `blob` of a `resource` block in `content`.
//!
//! - Each must be standard base64: the alphabet `A`–`Z`, `a`–`z`, `0`–`9`,
//!   `+` and `/`, a length that is a multiple of 4, `=` padding only at the
//!   end, bits that padding leaves unused at zero, and nothing else, no line
//!   break among it: the encoding every receiver of the report decodes.
//! - An image, an `image` block or a `resource` blob whose `mimeType` is
//!   `image/png`, `image/jpeg`, `image/gif` or `image/webp` and whose header
//!   shows that format with its width and height, that image acceptance
//!   refuses is replaced at its index by one text block naming its size and
//!   dimensions; every other block stays.
//! - The bounds are measured on the decoded bytes of what remains.

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use protocol::image::{Acceptance, Dimensions, Format, HEADER_MAX_BYTES, SIGNATURE_BYTES};
use protocol::tool::{ImageMime, Object};
use serde_json::{Value, json};

/// The media that remain in a checked result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Media {
    /// Their decoded bytes together.
    pub decoded: u64,
    /// The characters of their strings together: base64 needs no JSON
    /// escape, so emptying them shortens the compact JSON by this much.
    pub encoded: usize,
}

/// Checks every media string of `content` in order and replaces each
/// refused image; gives the media that remain, or the index of the first
/// block whose media is not valid base64.
pub(crate) fn check(content: &mut [Object]) -> Result<Media, usize> {
    let mut media = Media {
        decoded: 0,
        encoded: 0,
    };
    for (index, block) in content.iter_mut().enumerate() {
        let Some((string, mime)) = media_of(block) else {
            continue;
        };
        let decoded = STANDARD.decode(string).map_err(|_| index)?;
        let characters = string.len();
        if let Some(text) = mime.and_then(|mime| refusal(mime, &decoded)) {
            *block = text;
        } else {
            media.decoded += decoded.len() as u64;
            media.encoded += characters;
        }
    }
    Ok(media)
}

/// The block's media string and the `mimeType` that names it.
fn media_of(block: &Object) -> Option<(&str, Option<&str>)> {
    fn mime(object: &Object) -> Option<&str> {
        object.get("mimeType").and_then(Value::as_str)
    }
    match block.get("type").and_then(Value::as_str)? {
        "image" => Some((block.get("data")?.as_str()?, mime(block))),
        "audio" => Some((block.get("data")?.as_str()?, None)),
        "resource" => {
            let resource = block.get("resource")?.as_object()?;
            Some((resource.get("blob")?.as_str()?, mime(resource)))
        }
        _ => None,
    }
}

/// The text block that replaces an image of `mime` and `bytes` that image
/// acceptance refuses.
fn refusal(mime: &str, bytes: &[u8]) -> Option<Object> {
    let named: ImageMime = serde_json::from_value(json!(mime)).ok()?;
    let format = Format::of(&bytes[..bytes.len().min(SIGNATURE_BYTES)])?;
    if format.mime() != named {
        return None;
    }
    let dimensions = format.dimensions(&bytes[..bytes.len().min(HEADER_MAX_BYTES)])?;
    let size = bytes.len() as u64;
    Acceptance::default().check(size, dimensions).err()?;
    let Dimensions { width, height } = dimensions;
    let acceptance = Acceptance::default();
    let text = format!(
        "Image omitted: it is {size} bytes and {width}x{height} pixels, larger than the \
         supported models accept (at most {} bytes, {} pixels per side and {} patches of \
         32x32 pixels).",
        acceptance.max_bytes, acceptance.max_side, acceptance.max_patches
    );
    match json!({"type": "text", "text": text}) {
        Value::Object(block) => Some(block),
        _ => unreachable!("a JSON object"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn block(value: Value) -> Object {
        match value {
            Value::Object(block) => block,
            _ => panic!("an object"),
        }
    }

    #[test]
    fn media_strings_decode_only_as_padded_standard_base64() {
        for (data, valid) in [
            ("QQ==", true),
            ("QR==", false),
            ("", true),
            ("QQ", false),
            ("QQ==\n", false),
            ("Q\nQ==", false),
            ("-_8=", false),
            ("+/8=", true),
            ("QQ=A", false),
        ] {
            let mut content = vec![block(
                json!({"type": "audio", "data": data, "mimeType": "audio/wav"}),
            )];
            assert_eq!(check(&mut content).is_ok(), valid, "{data:?}");
        }
    }
}
