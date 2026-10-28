//! Images as the protocol carries them: the format recognized from an
//! image's first bytes, its width and height read from its header, and image
//! acceptance, which decides whether every supported model takes the image.

use crate::tool::ImageMime;

/// How many of an image's first bytes decide its format.
pub const SIGNATURE_BYTES: usize = 16;

/// How many of an image's first bytes its width and height must be found
/// in.
pub const HEADER_MAX_BYTES: usize = 1_048_576;

/// The side of the square patches that image acceptance counts, in pixels.
const PATCH_SIDE: u64 = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    Png,
    Jpeg,
    Gif,
    Webp,
}

impl Format {
    /// The format whose signature `bytes`, an image's first
    /// [`SIGNATURE_BYTES`] or fewer, begin with.
    pub fn of(bytes: &[u8]) -> Option<Format> {
        if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
            Some(Format::Png)
        } else if bytes.starts_with(&[0xff, 0xd8, 0xff]) {
            Some(Format::Jpeg)
        } else if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
            Some(Format::Gif)
        } else if bytes.len() >= 12 && bytes.starts_with(b"RIFF") && &bytes[8..12] == b"WEBP" {
            Some(Format::Webp)
        } else {
            None
        }
    }

    pub fn mime(self) -> ImageMime {
        match self {
            Format::Png => ImageMime::Png,
            Format::Jpeg => ImageMime::Jpeg,
            Format::Gif => ImageMime::Gif,
            Format::Webp => ImageMime::Webp,
        }
    }

    /// The width and height that `header`, the image's first bytes, gives:
    /// PNG's `IHDR`, GIF's logical screen descriptor, WebP's first `VP8 `,
    /// `VP8L` or `VP8X` chunk, JPEG's first start-of-frame segment. `None`
    /// when they are not within `header` or one of them is 0.
    pub fn dimensions(self, header: &[u8]) -> Option<Dimensions> {
        let (width, height) = match self {
            Format::Png => png(header)?,
            Format::Jpeg => jpeg(header)?,
            Format::Gif => (
                u32::from(u16::from_le_bytes(array(header, 6)?)),
                u32::from(u16::from_le_bytes(array(header, 8)?)),
            ),
            Format::Webp => webp(header)?,
        };
        (width > 0 && height > 0).then_some(Dimensions { width, height })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Dimensions {
    pub width: u32,
    pub height: u32,
}

/// The `N` bytes of `bytes` at `at`, when they are all there.
fn array<const N: usize>(bytes: &[u8], at: usize) -> Option<[u8; N]> {
    bytes.get(at..at.checked_add(N)?)?.try_into().ok()
}

fn png(header: &[u8]) -> Option<(u32, u32)> {
    // The first chunk after the signature is `IHDR`: length, type, width,
    // height.
    if array::<4>(header, 12)? != *b"IHDR" {
        return None;
    }
    Some((
        u32::from_be_bytes(array(header, 16)?),
        u32::from_be_bytes(array(header, 20)?),
    ))
}

fn jpeg(header: &[u8]) -> Option<(u32, u32)> {
    let mut at = 2;
    loop {
        if *header.get(at)? != 0xff {
            return None;
        }
        while *header.get(at)? == 0xff {
            at += 1;
        }
        let marker = header[at];
        at += 1;
        match marker {
            // Markers without a segment.
            0x01 | 0xd0..=0xd8 => continue,
            // The end of the image, or a scan, before any frame.
            0xd9 | 0xda => return None,
            _ => {}
        }
        let length = usize::from(u16::from_be_bytes(array(header, at)?));
        if length < 2 {
            return None;
        }
        // Every start-of-frame marker but DHT, JPG and DAC, which share
        // its range.
        if matches!(marker, 0xc0..=0xcf) && !matches!(marker, 0xc4 | 0xc8 | 0xcc) {
            let height = u16::from_be_bytes(array(header, at + 3)?);
            let width = u16::from_be_bytes(array(header, at + 5)?);
            return Some((u32::from(width), u32::from(height)));
        }
        at += length;
    }
}

fn webp(header: &[u8]) -> Option<(u32, u32)> {
    let mut at = 12;
    loop {
        let kind = array::<4>(header, at)?;
        let size = usize::try_from(u32::from_le_bytes(array(header, at + 4)?)).ok()?;
        let data = at + 8;
        match &kind {
            b"VP8 " => {
                if array::<3>(header, data + 3)? != [0x9d, 0x01, 0x2a] {
                    return None;
                }
                let width = u16::from_le_bytes(array(header, data + 6)?) & 0x3fff;
                let height = u16::from_le_bytes(array(header, data + 8)?) & 0x3fff;
                return Some((u32::from(width), u32::from(height)));
            }
            b"VP8L" => {
                if *header.get(data)? != 0x2f {
                    return None;
                }
                let bits = u32::from_le_bytes(array(header, data + 1)?);
                return Some(((bits & 0x3fff) + 1, ((bits >> 14) & 0x3fff) + 1));
            }
            b"VP8X" => {
                let side = |offset: usize| {
                    let [a, b, c] = array::<3>(header, data + offset)?;
                    Some(u32::from_le_bytes([a, b, c, 0]) + 1)
                };
                return Some((side(4)?, side(7)?));
            }
            // A chunk's data is padded to an even length.
            _ => at = data.checked_add(size)?.checked_add(size & 1)?,
        }
    }
}

pub const MAX_IMAGE_BYTES: u64 = 7_500_000;
pub const MAX_IMAGE_SIDE: u32 = 8_000;
pub const MAX_IMAGE_PATCHES: u64 = 30_000;

/// The bounds of image acceptance: the decoded size, each side, and the
/// patches of 32×32 pixels that the image covers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Acceptance {
    pub max_bytes: u64,
    pub max_side: u32,
    pub max_patches: u64,
}

impl Default for Acceptance {
    fn default() -> Acceptance {
        Acceptance {
            max_bytes: MAX_IMAGE_BYTES,
            max_side: MAX_IMAGE_SIDE,
            max_patches: MAX_IMAGE_PATCHES,
        }
    }
}

impl Acceptance {
    /// Accepts an image of `size_bytes` decoded bytes and `dimensions`, or
    /// gives the message of its refusal.
    pub fn check(&self, size_bytes: u64, dimensions: Dimensions) -> Result<(), String> {
        let Dimensions { width, height } = dimensions;
        let patches =
            u64::from(width).div_ceil(PATCH_SIDE) * u64::from(height).div_ceil(PATCH_SIDE);
        if size_bytes <= self.max_bytes
            && width <= self.max_side
            && height <= self.max_side
            && patches <= self.max_patches
        {
            return Ok(());
        }
        Err(format!(
            "image is {size_bytes} bytes and {width}x{height} pixels; the limit is {} bytes, {} \
             pixels per side and {} patches of {PATCH_SIDE}x{PATCH_SIDE} pixels. Shrink or \
             compress the image, then view it again.",
            self.max_bytes, self.max_side, self.max_patches
        ))
    }
}
