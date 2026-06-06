use pioneer_client::composer::{
    attachments::{
        ComposerAttachmentKind, append_composer_attachment_paths, composer_attachment_from_path,
    },
    turn_prepare::turn_input_from_composer_attachments,
};
use pioneer_protocol::UserInput;
use std::path::{Path, PathBuf};

fn main() {
    let screenshot = composer_attachment_from_path(Path::new("/tmp/screenshot.png"))
        .expect("valid screenshot attachment");
    assert_eq!(screenshot.kind, ComposerAttachmentKind::Image);

    let mut attachments = Vec::new();
    let changed = append_composer_attachment_paths(
        &mut attachments,
        [
            PathBuf::from("/tmp/screenshot.png"),
            PathBuf::from("/tmp/notes.md"),
        ],
    );

    assert!(changed);
    assert_eq!(attachments.len(), 2);

    let input = turn_input_from_composer_attachments("Summarize these files", &attachments);
    assert_eq!(input.len(), 3);
    assert!(matches!(&input[0], UserInput::Text { text, .. } if text == "Summarize these files"));
    assert!(matches!(&input[1], UserInput::LocalImage { path } if path == "/tmp/screenshot.png"));
    assert!(matches!(&input[2], UserInput::LocalFile { path } if path == "/tmp/notes.md"));

    println!("prepared {} user input items", input.len());
}
