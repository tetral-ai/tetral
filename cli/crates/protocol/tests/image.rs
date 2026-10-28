//! The format and the dimensions that image acceptance reads, from images
//! that encoders wrote: one of each format, JPEG both baseline and
//! progressive, and WebP with each of its three kinds of first chunk.

use protocol::image::{Dimensions, Format, SIGNATURE_BYTES};

/// The image file `name` of `tests/images/` with its format.
macro_rules! image {
    ($name:literal, $format:ident) => {
        (
            $name,
            &include_bytes!(concat!("images/", $name))[..],
            Format::$format,
        )
    };
}

#[test]
fn the_format_and_dimensions_of_each_kind_of_image_are_read_from_its_header() {
    let images = [
        image!("image.png", Png),
        image!("baseline.jpg", Jpeg),
        image!("progressive.jpg", Jpeg),
        image!("image.gif", Gif),
        image!("lossy.webp", Webp),
        image!("lossless.webp", Webp),
        image!("extended.webp", Webp),
    ];
    let encoded = Dimensions {
        width: 33,
        height: 17,
    };
    for (name, bytes, format) in images {
        let signature = &bytes[..SIGNATURE_BYTES];
        assert_eq!(Format::of(signature), Some(format), "{name}");
        assert_eq!(format.dimensions(bytes), Some(encoded), "{name}");
    }
}
