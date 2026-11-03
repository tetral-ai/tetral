//! `/proc/self/mountinfo`: the device number of the mount at a path, read
//! without touching the path itself.

use std::path::Path;

/// The device number (`major:minor`, field 3) of the last line whose mount
/// point (field 5, octal escapes decoded) is `point`.
pub fn device_of(mountinfo: &str, point: &Path) -> Option<String> {
    let point = point.as_os_str().as_encoded_bytes();
    mountinfo.lines().rev().find_map(|line| {
        let fields: Vec<&str> = line.split(' ').collect();
        let (device, mount_point) = (fields.get(2)?, fields.get(4)?);
        (decoded(mount_point) == point).then(|| (*device).to_owned())
    })
}

/// The device number of the mount at `point` as this process sees it now.
pub fn current_device(point: &Path) -> std::io::Result<Option<String>> {
    let mountinfo = std::fs::read_to_string("/proc/self/mountinfo")?;
    Ok(device_of(&mountinfo, point))
}

/// `field` with each `\ooo` octal escape replaced by its byte.
fn decoded(field: &str) -> Vec<u8> {
    let bytes = field.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut at = 0;
    while at < bytes.len() {
        let escape = bytes.get(at + 1..at + 4).filter(|digits| {
            bytes[at] == b'\\' && digits.iter().all(|digit| (b'0'..=b'7').contains(digit))
        });
        match escape {
            Some(digits) => {
                let value = digits
                    .iter()
                    .fold(0u32, |value, digit| value * 8 + u32::from(digit - b'0'));
                out.push(value as u8);
                at += 4;
            }
            None => {
                out.push(bytes[at]);
                at += 1;
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_last_line_of_an_escaped_mount_point_gives_its_device() {
        let mountinfo = "\
22 1 8:1 / / rw - ext4 /dev/sda1 rw
40 22 0:51 / /home/a\\040b/drives/d rw - fuse.tetral tetral rw
41 22 0:57 / /home/a\\040b/drives/d rw - fuse.tetral tetral rw
42 22 0:58 / /home/a\\040b/drives/dd rw - fuse.tetral tetral rw";
        assert_eq!(
            device_of(mountinfo, Path::new("/home/a b/drives/d")),
            Some("0:57".to_owned())
        );
        assert_eq!(device_of(mountinfo, Path::new("/home/a b/drives")), None);
    }
}
