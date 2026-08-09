use rustrict::CensorStr;

/// Censor inappropriate chat while preserving benign messages verbatim.
pub(crate) fn censor_chat_message(message: &str) -> String {
    message.censor()
}

#[cfg(test)]
mod tests {
    use super::censor_chat_message;

    #[test]
    fn benign_chat_is_unchanged() {
        let message = "Good luck, have fun!";

        assert_eq!(censor_chat_message(message), message);
    }

    #[test]
    fn obvious_profanity_is_censored() {
        assert_eq!(censor_chat_message("hello crap"), "hello c***");
    }

    #[test]
    fn profanity_evasion_is_censored() {
        let censored = censor_chat_message("what the f u c k");

        assert_ne!(censored, "what the f u c k");
        assert!(censored.contains('*'));
    }
}
