use anyhow::Result;

/// Clipboard wrapper for terminal copy/paste operations.
///
/// Provides a simple interface to interact with the system clipboard,
/// supporting both X11 and Wayland on Linux through the arboard crate.
///
/// # Examples
///
/// ```no_run
/// use terminal::clipboard::Clipboard;
///
/// let mut clipboard = Clipboard::new().unwrap();
///
/// // Copy text to clipboard
/// clipboard.copy("Hello, World!").unwrap();
///
/// // Paste text from clipboard
/// let text = clipboard.paste().unwrap();
/// println!("Clipboard contents: {}", text);
/// ```
pub struct Clipboard {
    clipboard: arboard::Clipboard,
}

impl Clipboard {
    /// Creates a new clipboard instance.
    ///
    /// This initializes the connection to the system clipboard.
    /// On Wayland, this uses the wayland-data-control protocol.
    ///
    /// # Errors
    ///
    /// Returns an error if the clipboard cannot be initialized,
    /// which may happen if:
    /// - The display server is not accessible
    /// - Required permissions are not available
    /// - The clipboard system is not supported
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use terminal::clipboard::Clipboard;
    ///
    /// match Clipboard::new() {
    ///     Ok(clipboard) => println!("Clipboard initialized"),
    ///     Err(e) => eprintln!("Failed to initialize clipboard: {}", e),
    /// }
    /// ```
    pub fn new() -> Result<Self> {
        Ok(Self {
            clipboard: arboard::Clipboard::new()?,
        })
    }

    /// Copies text to the system clipboard.
    ///
    /// This replaces the current clipboard contents with the provided text.
    ///
    /// # Arguments
    ///
    /// * `text` - The text to copy to the clipboard
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The clipboard is not accessible
    /// - Permission to write to the clipboard is denied
    /// - The clipboard connection has been lost
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use terminal::clipboard::Clipboard;
    ///
    /// let mut clipboard = Clipboard::new().unwrap();
    /// clipboard.copy("Selected terminal text").unwrap();
    /// ```
    pub fn copy(&mut self, text: &str) -> Result<()> {
        self.clipboard.set_text(text)?;
        Ok(())
    }

    /// Pastes text from the system clipboard.
    ///
    /// Retrieves the current text content from the clipboard.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The clipboard is not accessible
    /// - Permission to read from the clipboard is denied
    /// - The clipboard is empty
    /// - The clipboard contains non-text content
    /// - The clipboard connection has been lost
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use terminal::clipboard::Clipboard;
    ///
    /// let mut clipboard = Clipboard::new().unwrap();
    /// match clipboard.paste() {
    ///     Ok(text) => println!("Pasted: {}", text),
    ///     Err(e) => eprintln!("Failed to paste: {}", e),
    /// }
    /// ```
    pub fn paste(&mut self) -> Result<String> {
        Ok(self.clipboard.get_text()?)
    }

    /// Clears the clipboard contents.
    ///
    /// This removes all content from the system clipboard.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The clipboard is not accessible
    /// - Permission to modify the clipboard is denied
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use terminal::clipboard::Clipboard;
    ///
    /// let mut clipboard = Clipboard::new().unwrap();
    /// clipboard.clear().unwrap();
    /// ```
    pub fn clear(&mut self) -> Result<()> {
        self.clipboard.clear()?;
        Ok(())
    }
}

impl Default for Clipboard {
    /// Creates a new clipboard instance using the default constructor.
    ///
    /// # Panics
    ///
    /// Panics if the clipboard cannot be initialized. For error handling,
    /// use [`Clipboard::new()`] instead.
    fn default() -> Self {
        Self::new().expect("Failed to initialize clipboard")
    }
}
