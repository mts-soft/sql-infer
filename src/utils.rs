pub fn to_pascal(mixed_case_name: &str) -> String {
    let mut words = vec![];
    let mut curr = String::new();
    for character in mixed_case_name.chars() {
        let is_snake = character == '_';
        if character.is_uppercase() || is_snake {
            words.push(curr.clone());
            curr.clear();
        }
        if is_snake {
            continue;
        }
        if curr.is_empty() {
            curr.push(character.to_ascii_uppercase());
        } else {
            curr.push(character.to_ascii_lowercase());
        }
    }
    words.push(curr);
    words.join("")
}
