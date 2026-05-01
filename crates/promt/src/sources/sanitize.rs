pub fn sanitize_file_content(input: &str) -> String {
    let normalized_newlines = input.replace("\r\n", "\n").replace('\r', "\n");
    normalized_newlines.trim().to_owned()
}
