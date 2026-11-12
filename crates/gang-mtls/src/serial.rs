//! Random certificate serial numbers and their conversion to `CertSerial` and ASN.1.

use gang_types::CertSerial;
use x509_cert::serial_number::SerialNumber;

use crate::errors::MtlsError;

const SERIAL_BYTES: usize = 16;

/// A 16-byte certificate serial number that is always a positive, 16-byte DER integer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Serial([u8; SERIAL_BYTES]);

impl Serial {
    /// Draws a fresh random serial from the operating system.
    pub(crate) fn generate() -> Result<Self, MtlsError> {
        let mut bytes = [0u8; SERIAL_BYTES];
        getrandom::fill(&mut bytes).map_err(|_| MtlsError::Entropy)?;
        // Clearing the sign bit keeps the DER integer positive; setting the next bit keeps it
        // at 16 bytes without a leading zero octet.
        bytes[0] = (bytes[0] & 0x7f) | 0x40;
        Ok(Self(bytes))
    }

    /// Returns the serial as the lowercase hexadecimal `CertSerial` used by the ports.
    pub(crate) fn cert_serial(&self) -> Result<CertSerial, MtlsError> {
        Ok(CertSerial::from_be_bytes(&self.0)?)
    }

    /// Returns the serial as an X.509 `INTEGER`.
    pub(crate) fn to_asn1(self) -> Result<SerialNumber, MtlsError> {
        SerialNumber::new(&self.0).map_err(MtlsError::Encoding)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cert_serial_is_lowercase_hex() {
        let bytes = [
            0x40, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
            0xfe, 0xff,
        ];
        let serial = Serial(bytes);

        let cert_serial = serial.cert_serial().expect("hex serial is valid");

        assert_eq!(cert_serial.as_str(), "400102030405060708090a0b0c0dfeff");
    }

    #[test]
    fn generated_serials_have_positive_sixteen_byte_form() {
        for _ in 0..64 {
            let serial = Serial::generate().expect("operating system randomness is available");
            assert_eq!(serial.0[0] & 0x80, 0, "sign bit must be clear");
            assert_eq!(serial.0[0] & 0x40, 0x40, "second bit must be set");
            assert_eq!(
                serial
                    .cert_serial()
                    .expect("hex serial is valid")
                    .as_str()
                    .len(),
                32
            );
        }
    }
}
