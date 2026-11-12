//! Instruction set architectures.

crate::string_enum! {
    /// An instruction set architecture a worker can run.
    pub enum Isa("isa") {
        /// 64-bit Arm.
        Aarch64 = "aarch64",
        /// 64-bit x86.
        X86_64 = "x86-64",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_and_displays_isa_names() {
        assert_eq!("aarch64".parse::<Isa>(), Ok(Isa::Aarch64));
        assert_eq!("x86-64".parse::<Isa>(), Ok(Isa::X86_64));
    }

    #[test]
    fn unknown_isa_is_rejected() {
        assert!("riscv".parse::<Isa>().is_err());
    }

    #[test]
    fn serde_uses_the_canonical_names() {
        assert!(serde_json::from_str::<Isa>("\"x86_64\"").is_err());
        assert_eq!(
            serde_json::from_str::<Isa>("\"x86-64\"").expect("canonical name parses"),
            Isa::X86_64
        );
    }
}
