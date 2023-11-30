use bincode::{Decode, Encode};
use snap::raw::{Decoder, Encoder};
/// Data Encoding and Compression (DEC)
#[derive(Debug)]
pub(crate) struct Dec {
    encoder: Option<snap::raw::Encoder>,
    decoder: Option<snap::raw::Decoder>,
}

impl Dec {
    /// Create a new Data Encoder and Compressor
    pub fn new(compress: bool) -> Dec {
        let (encoder, decoder) = if compress {
            (Some(Encoder::new()), Some(Decoder::new()))
        } else {
            (None, None)
        };

        Self { encoder, decoder }
    }
    /// Deserializes a slice of bytes into an instance of `T`
    pub fn deser<T: Decode>(
        &mut self,
        bytes: &[u8],
    ) -> crate::error::GhalaDbResult<T> {
        let t: T = if let Some(ref mut dcr) = self.decoder {
            Self::deser_raw(&dcr.decompress_vec(bytes)?)?
        } else {
            Self::deser_raw(bytes)?
        };
        Ok(t)
    }
    /// Deserializes a slice of bytes into an instance of `T` without
    /// decompressing
    pub fn deser_raw<T: Decode>(bytes: &[u8]) -> crate::error::GhalaDbResult<T> {
        Ok(bincode::decode_from_slice(bytes, Self::conf())?.0)
    }

    /// Serializes a serializable object into a `Vec` of bytes
    pub fn ser<T: ?Sized + Encode>(
        &mut self,
        value: &T,
    ) -> crate::error::GhalaDbResult<Vec<u8>> {
        let bytes = Self::ser_raw(value)?;
        let ret = if let Some(ref mut enc) = self.encoder {
            enc.compress_vec(&bytes)?
        } else {
            bytes
        };
        Ok(ret)
    }
    /// Serializes a serializable object into a `Vec` of bytes without
    /// compression
    pub fn ser_raw<T: ?Sized + Encode>(
        value: &T,
    ) -> crate::error::GhalaDbResult<Vec<u8>> {
        Ok(bincode::encode_to_vec(value, Self::conf())?)
    }

    #[inline]
    fn conf() -> impl bincode::config::Config {
        bincode::config::standard()
            .with_little_endian()
            .with_fixed_int_encoding()
            .with_no_limit()
    }
}
