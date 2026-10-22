//! PDF layout extraction utilities.
//!
//! This module extracts text boxes and line segments from PDFs,
//! providing the raw layout data needed for table detection.

use crate::error::{MemvidError, Result};

/// A text box with position information from a PDF.
#[derive(Debug, Clone)]
pub struct TextBox {
    /// Text content
    pub text: String,
    /// Left edge X coordinate (in points)
    pub x: f32,
    /// Bottom edge Y coordinate (in points, PDF coordinates)
    pub y: f32,
    /// Width in points
    pub width: f32,
    /// Height in points
    pub height: f32,
    /// Font size (if available)
    pub font_size: f32,
    /// Page number (1-indexed)
    pub page: u32,
}

impl TextBox {
    /// Get the right edge X coordinate.
    #[must_use]
    pub fn right(&self) -> f32 {
        self.x + self.width
    }

    /// Get the top edge Y coordinate.
    #[must_use]
    pub fn top(&self) -> f32 {
        self.y + self.height
    }

    /// Get the center X coordinate.
    #[must_use]
    pub fn center_x(&self) -> f32 {
        self.x + self.width / 2.0
    }

    /// Get the center Y coordinate.
    #[must_use]
    pub fn center_y(&self) -> f32 {
        self.y + self.height / 2.0
    }

    /// Check if this text box overlaps with another.
    #[must_use]
    pub fn overlaps(&self, other: &Self) -> bool {
        self.x < other.right()
            && self.right() > other.x
            && self.y < other.top()
            && self.top() > other.y
    }
}

/// A line segment from PDF path objects (for Lattice detection).
#[derive(Debug, Clone)]
pub struct LineSegment {
    /// Start X coordinate
    pub x1: f32,
    /// Start Y coordinate
    pub y1: f32,
    /// End X coordinate
    pub x2: f32,
    /// End Y coordinate
    pub y2: f32,
    /// Page number (1-indexed)
    pub page: u32,
}

impl LineSegment {
    /// Check if this is a horizontal line (within tolerance).
    #[must_use]
    pub fn is_horizontal(&self, tolerance: f32) -> bool {
        (self.y1 - self.y2).abs() <= tolerance
    }

    /// Check if this is a vertical line (within tolerance).
    #[must_use]
    pub fn is_vertical(&self, tolerance: f32) -> bool {
        (self.x1 - self.x2).abs() <= tolerance
    }

    /// Get the length of this line segment.
    #[must_use]
    pub fn length(&self) -> f32 {
        ((self.x2 - self.x1).powi(2) + (self.y2 - self.y1).powi(2)).sqrt()
    }

    /// Get the Y coordinate for horizontal lines.
    #[must_use]
    pub fn y_coord(&self) -> f32 {
        f32::midpoint(self.y1, self.y2)
    }

    /// Get the X coordinate for vertical lines.
    #[must_use]
    pub fn x_coord(&self) -> f32 {
        f32::midpoint(self.x1, self.x2)
    }
}

/// Layout information for a single page.
#[derive(Debug, Clone)]
pub struct PageLayout {
    /// Page number (1-indexed)
    pub page_number: u32,
    /// Page width in points
    pub width: f32,
    /// Page height in points
    pub height: f32,
    /// Text boxes on this page
    pub text_boxes: Vec<TextBox>,
    /// Line segments on this page (for Lattice detection)
    pub lines: Vec<LineSegment>,
}

impl PageLayout {
    /// Create an empty page layout.
    #[must_use]
    pub fn new(page_number: u32, width: f32, height: f32) -> Self {
        Self {
            page_number,
            width,
            height,
            text_boxes: Vec::new(),
            lines: Vec::new(),
        }
    }

    /// Check if the page has any content.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.text_boxes.is_empty() && self.lines.is_empty()
    }

    /// Get horizontal lines (filtered by tolerance).
    #[must_use]
    pub fn horizontal_lines(&self, tolerance: f32) -> Vec<&LineSegment> {
        self.lines
            .iter()
            .filter(|l| l.is_horizontal(tolerance))
            .collect()
    }

    /// Get vertical lines (filtered by tolerance).
    #[must_use]
    pub fn vertical_lines(&self, tolerance: f32) -> Vec<&LineSegment> {
        self.lines
            .iter()
            .filter(|l| l.is_vertical(tolerance))
            .collect()
    }

    /// Check if this page likely has ruled tables (significant line count).
    #[must_use]
    pub fn has_ruled_structure(&self, min_lines: usize, tolerance: f32) -> bool {
        let h_count = self.horizontal_lines(tolerance).len();
        let v_count = self.vertical_lines(tolerance).len();
        h_count >= min_lines && v_count >= min_lines
    }
}

/// Extract layout information from a PDF using pdfium.
///
/// This is the primary extraction path when the `pdfium` feature is enabled.
#[cfg(feature = "pdfium")]
pub fn extract_pdf_layout(bytes: &[u8], max_pages: usize) -> Result<Vec<PageLayout>> {
    use pdfium_render::prelude::*;

    let pdfium = Pdfium::default();
    let document =
        pdfium
            .load_pdf_from_byte_slice(bytes, None)
            .map_err(|e| MemvidError::TableExtraction {
                reason: format!("failed to load PDF: {e}"),
            })?;

    let page_count = document.pages().len() as usize;
    let max_pages_usize = max_pages as usize;
    let pages_to_process = if max_pages_usize > 0 {
        page_count.min(max_pages_usize)
    } else {
        page_count
    };

    let mut layouts = Vec::with_capacity(pages_to_process);

    for page_idx in 0..pages_to_process {
        let page =
            document
                .pages()
                .get(page_idx as u16)
                .map_err(|e| MemvidError::TableExtraction {
                    reason: format!("failed to get page {}: {e}", page_idx + 1),
                })?;

        let page_number = (page_idx + 1) as u32;
        let width = page.width().value;
        let height = page.height().value;

        let mut layout = PageLayout::new(page_number, width, height);

        // Extract text objects with positions
        for object in page.objects().iter() {
            if let Some(text_obj) = object.as_text_object() {
                if let Ok(bounds) = object.bounds() {
                    let text = text_obj.text();
                    if !text.trim().is_empty() {
                        layout.text_boxes.push(TextBox {
                            text,
                            x: bounds.left().value,
                            y: bounds.bottom().value,
                            width: bounds.right().value - bounds.left().value,
                            height: bounds.top().value - bounds.bottom().value,
                            font_size: text_obj.unscaled_font_size().value,
                            page: page_number,
                        });
                    }
                }
            }

            // Extract path objects for line detection
            if let Some(path_obj) = object.as_path_object() {
                extract_lines_from_path(&path_obj, page_number, &mut layout.lines);
            }
        }

        layouts.push(layout);
    }

    Ok(layouts)
}

/// Extract line segments from a PDF path object.
#[cfg(feature = "pdfium")]
fn extract_lines_from_path(
    path: &pdfium_render::prelude::PdfPagePathObject,
    page: u32,
    lines: &mut Vec<LineSegment>,
) {
    use pdfium_render::prelude::*;

    let mut current_x = 0.0f32;
    let mut current_y = 0.0f32;

    for segment in path.segments().iter() {
        match segment.segment_type() {
            PdfPathSegmentType::MoveTo => {
                // API changed: x() and y() return PdfPoints directly now
                let x = segment.x();
                let y = segment.y();
                current_x = x.value;
                current_y = y.value;
            }
            PdfPathSegmentType::LineTo => {
                let x = segment.x();
                let y = segment.y();
                let new_x = x.value;
                let new_y = y.value;

                // Only add lines of significant length
                let length = ((new_x - current_x).powi(2) + (new_y - current_y).powi(2)).sqrt();
                if length > 5.0 {
                    lines.push(LineSegment {
                        x1: current_x,
                        y1: current_y,
                        x2: new_x,
                        y2: new_y,
                        page,
                    });
                }

                current_x = new_x;
                current_y = new_y;
            }
            PdfPathSegmentType::BezierTo => {
                // For Bezier curves, just move to the endpoint
                // (curves are rarely used for table borders)
                let x = segment.x();
                let y = segment.y();
                current_x = x.value;
                current_y = y.value;
            }
            _ => {
                // Handles Unknown and any other segment types (like close path)
                // For close path: connect back to move point
                // For unknown: ignore
            }
        }
    }
}

/// Fallback layout extraction using lopdf when pdfium is not available.
///
/// This provides basic text extraction with whitespace-based column detection.
/// While not as accurate as pdfium's native text positioning, it can still
/// detect tables with consistent column alignment.
#[cfg(not(feature = "pdfium"))]
pub fn extract_pdf_layout(bytes: &[u8], max_pages: usize) -> Result<Vec<PageLayout>> {
    use lopdf::Document;

    let document = Document::load_mem(bytes).map_err(|e| MemvidError::TableExtraction {
        reason: format!("failed to load PDF with lopdf: {e}"),
    })?;

    let page_count = document.get_pages().len();
    let pages_to_process = if max_pages > 0 {
        page_count.min(max_pages)
    } else {
        page_count
    };

    let mut layouts = Vec::with_capacity(pages_to_process);

    for page_idx in 0..pages_to_process {
        let page_number = u32::try_from(page_idx + 1).unwrap_or(0);

        // Get page dimensions (default to standard US Letter if not available)
        let (width, height) = get_page_dimensions(&document, page_idx).unwrap_or((612.0, 792.0));

        let mut layout = PageLayout::new(page_number, width, height);

        // Extract text and parse whitespace-delimited columns
        if let Ok(text) = document.extract_text(&[page_number]) {
            let lines: Vec<&str> = text.lines().collect();
            let line_height = if lines.is_empty() {
                12.0
            } else {
                (height - 144.0) / lines.len() as f32 // Leave margins
            };

            for (line_idx, line) in lines.iter().enumerate() {
                if line.trim().is_empty() {
                    continue;
                }

                // Parse the line into whitespace-separated columns
                // Use multiple spaces as column delimiter (common in PDF text extraction)
                let text_boxes = parse_line_into_columns(
                    line,
                    line_idx,
                    page_number,
                    width,
                    height,
                    line_height,
                );

                layout.text_boxes.extend(text_boxes);
            }
        }

        // lopdf doesn't easily expose path objects for line detection
        layout.lines = Vec::new();

        layouts.push(layout);
    }

    Ok(layouts)
}

/// Parse a line into separate text boxes based on whitespace patterns.
///
/// This uses multiple spaces (2+) as column delimiters, which is common
/// in PDF text extraction output for tabular data.
#[cfg(not(feature = "pdfium"))]
fn parse_line_into_columns(
    line: &str,
    line_idx: usize,
    page: u32,
    page_width: f32,
    page_height: f32,
    line_height: f32,
) -> Vec<TextBox> {
    let mut boxes = Vec::new();
    let y = page_height - 72.0 - (line_idx as f32 * line_height);

    // Split on 2+ spaces to detect column boundaries
    let re_split: Vec<&str> = line.split("  ").collect();

    if re_split.len() > 1 {
        // Multiple columns detected - assign positions based on split points
        let usable_width = page_width - 144.0; // Leave 1-inch margins on each side
        let col_width = usable_width / re_split.len() as f32;

        for (col_idx, col_text) in re_split.iter().enumerate() {
            let trimmed = col_text.trim();
            if !trimmed.is_empty() {
                let x = 72.0 + (col_idx as f32 * col_width);
                boxes.push(TextBox {
                    text: trimmed.to_string(),
                    x,
                    y,
                    width: col_width * 0.9, // Slightly smaller than slot
                    height: line_height,
                    font_size: 12.0,
                    page,
                });
            }
        }
    } else {
        // Single column - try splitting on tabs or check for number patterns
        let tab_split: Vec<&str> = line.split('\t').collect();

        if tab_split.len() > 1 {
            // Tab-separated
            let usable_width = page_width - 144.0;
            let col_width = usable_width / tab_split.len() as f32;

            for (col_idx, col_text) in tab_split.iter().enumerate() {
                let trimmed = col_text.trim();
                if !trimmed.is_empty() {
                    let x = 72.0 + (col_idx as f32 * col_width);
                    boxes.push(TextBox {
                        text: trimmed.to_string(),
                        x,
                        y,
                        width: col_width * 0.9,
                        height: line_height,
                        font_size: 12.0,
                        page,
                    });
                }
            }
        } else {
            // Single text span - place at left margin
            let trimmed = line.trim();
            if !trimmed.is_empty() {
                boxes.push(TextBox {
                    text: trimmed.to_string(),
                    x: 72.0,
                    y,
                    width: page_width - 144.0,
                    height: line_height,
                    font_size: 12.0,
                    page,
                });
            }
        }
    }

    boxes
}

/// Get page dimensions from a lopdf document.
#[cfg(not(feature = "pdfium"))]
fn get_page_dimensions(document: &lopdf::Document, page_idx: usize) -> Option<(f32, f32)> {
    let pages = document.get_pages();
    let page_id = *pages.get(&u32::try_from(page_idx + 1).unwrap_or(0))?;

    if let Ok(page) = document.get_dictionary(page_id) {
        if let Ok(media_box) = page.get(b"MediaBox") {
            if let lopdf::Object::Array(arr) = media_box {
                if arr.len() >= 4 {
                    let width = match &arr[2] {
                        lopdf::Object::Integer(n) => *n as f32,
                        lopdf::Object::Real(n) => *n,
                        _ => return None,
                    };
                    let height = match &arr[3] {
                        lopdf::Object::Integer(n) => *n as f32,
                        lopdf::Object::Real(n) => *n,
                        _ => return None,
                    };
                    return Some((width, height));
                }
            }
        }
    }
    None
}

/// Cluster values using a simple distance-based algorithm.
///
/// Groups values that are within `threshold` of each other
/// and returns cluster centroids.
#[must_use]
pub fn cluster_values(values: &[f32], threshold: f32) -> Vec<f32> {
    if values.is_empty() {
        return Vec::new();
    }

    let mut sorted: Vec<f32> = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    let mut clusters: Vec<Vec<f32>> = Vec::new();
    let mut current_cluster = vec![sorted[0]];

    for &val in &sorted[1..] {
        let last = current_cluster.last().copied().unwrap_or(val);
        if val - last <= threshold {
            current_cluster.push(val);
        } else {
            clusters.push(current_cluster);
            current_cluster = vec![val];
        }
    }

    if !current_cluster.is_empty() {
        clusters.push(current_cluster);
    }

    // Return cluster centroids
    clusters
        .iter()
        .map(|cluster| cluster.iter().sum::<f32>() / cluster.len() as f32)
        .collect()
}

/// Filter cluster values to keep only those appearing consistently.
#[allow(dead_code)]
pub fn filter_consistent_values(
    candidates: &[f32],
    reference_values: &[f32],
    threshold: f32,
    min_occurrences: usize,
) -> Vec<f32> {
    candidates
        .iter()
        .filter(|&&candidate| {
            let count = reference_values
                .iter()
                .filter(|&&v| (v - candidate).abs() <= threshold)
                .count();
            count >= min_occurrences
        })
        .copied()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_text_box_geometry() {
        let tbox = TextBox {
            text: "Hello".to_string(),
            x: 100.0,
            y: 200.0,
            width: 50.0,
            height: 20.0,
            font_size: 12.0,
            page: 1,
        };

        assert!((tbox.right() - 150.0).abs() < 0.001);
        assert!((tbox.top() - 220.0).abs() < 0.001);
        assert!((tbox.center_x() - 125.0).abs() < 0.001);
        assert!((tbox.center_y() - 210.0).abs() < 0.001);
    }

    #[test]
    fn test_line_segment_orientation() {
        let h_line = LineSegment {
            x1: 0.0,
            y1: 100.0,
            x2: 200.0,
            y2: 100.0,
            page: 1,
        };
        assert!(h_line.is_horizontal(1.0));
        assert!(!h_line.is_vertical(1.0));

        let v_line = LineSegment {
            x1: 100.0,
            y1: 0.0,
            x2: 100.0,
            y2: 200.0,
            page: 1,
        };
        assert!(!v_line.is_horizontal(1.0));
        assert!(v_line.is_vertical(1.0));
    }

    #[test]
    fn test_cluster_values() {
        let values = vec![10.0, 11.0, 12.0, 50.0, 51.0, 100.0];
        let clusters = cluster_values(&values, 5.0);

        assert_eq!(clusters.len(), 3);
        // First cluster around 11
        assert!((clusters[0] - 11.0).abs() < 1.0);
        // Second cluster around 50.5
        assert!((clusters[1] - 50.5).abs() < 1.0);
        // Third cluster at 100
        assert!((clusters[2] - 100.0).abs() < 1.0);
    }

    #[test]
    fn test_page_layout_line_filtering() {
        let mut layout = PageLayout::new(1, 612.0, 792.0);
        layout.lines.push(LineSegment {
            x1: 0.0,
            y1: 100.0,
            x2: 200.0,
            y2: 100.0,
            page: 1,
        }); // horizontal
        layout.lines.push(LineSegment {
            x1: 100.0,
            y1: 0.0,
            x2: 100.0,
            y2: 200.0,
            page: 1,
        }); // vertical
        layout.lines.push(LineSegment {
            x1: 0.0,
            y1: 0.0,
            x2: 200.0,
            y2: 200.0,
            page: 1,
        }); // diagonal

        assert_eq!(layout.horizontal_lines(2.0).len(), 1);
        assert_eq!(layout.vertical_lines(2.0).len(), 1);
    }
}
