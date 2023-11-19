use snap::raw::{Decoder, Encoder};
/// Data Encoding and Compression (DEC)
#[derive(Debug)]
pub(crate) struct Dec {
    #[allow(unused)]
    compress: bool,
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

        Self {
            compress,
            encoder,
            decoder,
        }
    }
    /// Deserializes a slice of bytes into an instance of `T`
    pub fn deser<T>(&mut self, bytes: &[u8]) -> crate::error::GhalaDbResult<T>
    where
        T: serde::de::DeserializeOwned,
    {
        let t: T = if let Some(ref mut dcr) = self.decoder {
            Self::deser_raw(&dcr.decompress_vec(bytes)?)?
        } else {
            Self::deser_raw(bytes)?
        };
        Ok(t)
    }
    /// Deserializes a slice of bytes into an instance of `T` without decompressing
    pub fn deser_raw<T>(bytes: &[u8]) -> crate::error::GhalaDbResult<T>
    where
        T: serde::de::DeserializeOwned,
    {
        let config = Self::serde_config();

        Ok(bincode::serde::decode_from_slice(bytes, config)?.0)
    }

    /// Serializes a serializable object into a `Vec` of bytes
    pub fn ser<T: ?Sized>(
        &mut self,
        value: &T,
    ) -> crate::error::GhalaDbResult<Vec<u8>>
    where
        T: serde::Serialize,
    {
        let bytes = Self::ser_raw(value)?;
        let ret = if let Some(ref mut enc) = self.encoder {
            enc.compress_vec(&bytes)?
        } else {
            bytes
        };
        Ok(ret)
    }
    /// Serializes a serializable object into a `Vec` of bytes without compression
    pub fn ser_raw<T: ?Sized>(value: &T) -> crate::error::GhalaDbResult<Vec<u8>>
    where
        T: serde::Serialize,
    {
        let config = Self::serde_config();
        Ok(bincode::serde::encode_to_vec(value, config)?)
    }

    #[inline]
    fn serde_config() -> impl bincode::config::Config {
        bincode::config::standard()
            .with_little_endian()
            .with_fixed_int_encoding()
            .with_no_limit()
    }
}
