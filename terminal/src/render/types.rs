use ratatui::style::Style;

#[derive(Clone, Copy, Debug)]
pub struct RenderConfig {
    pub chars_per_point: CharDimensions,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CharDimensions {
    pub horizontal: usize,
    pub vertical: usize,
}

impl CharDimensions {
    pub fn new(horizontal: usize, vertical: usize) -> Self {
        Self {
            horizontal,
            vertical,
        }
    }
}

pub struct CharGrid {
    grid: Vec<Vec<char>>,
    styles: Vec<Vec<Style>>,
    logical_width: usize,
    logical_height: usize,
    char_dims: CharDimensions,
}

impl CharGrid {
    pub fn new(logical_width: usize, logical_height: usize, char_dims: CharDimensions) -> Self {
        let physical_width = logical_width * char_dims.horizontal;
        let physical_height = logical_height * char_dims.vertical;
        let grid = vec![vec![' '; physical_width]; physical_height];
        let styles = vec![vec![Style::default(); physical_width]; physical_height];
        Self {
            grid,
            styles,
            logical_width,
            logical_height,
            char_dims,
        }
    }

    pub fn set_logical_point(&mut self, x: usize, y: usize, pattern: &CharPattern) {
        let start_x = x * self.char_dims.horizontal;
        let start_y = y * self.char_dims.vertical;

        for (dy, (char_row, style_row)) in
            pattern.chars.iter().zip(pattern.styles.iter()).enumerate()
        {
            for (dx, (&ch, &style)) in char_row.iter().zip(style_row.iter()).enumerate() {
                if let Some(grid_row) = self.grid.get_mut(start_y + dy) {
                    if let Some(cell) = grid_row.get_mut(start_x + dx) {
                        *cell = ch;
                    }
                }
                if let Some(style_grid_row) = self.styles.get_mut(start_y + dy) {
                    if let Some(style_cell) = style_grid_row.get_mut(start_x + dx) {
                        *style_cell = style;
                    }
                }
            }
        }
    }

    pub fn into_lines(self) -> Vec<Vec<char>> {
        self.grid
    }

    pub fn into_styled_lines(self) -> Vec<(Vec<char>, Vec<Style>)> {
        self.grid.into_iter().zip(self.styles.into_iter()).collect()
    }

    pub fn physical_width(&self) -> usize {
        self.logical_width * self.char_dims.horizontal
    }

    pub fn physical_height(&self) -> usize {
        self.logical_height * self.char_dims.vertical
    }
}

#[derive(Clone, Debug)]
pub struct CharPattern {
    pub chars: Vec<Vec<char>>,
    pub styles: Vec<Vec<Style>>,
}

impl CharPattern {
    pub fn new(chars: Vec<Vec<char>>) -> Self {
        let height = chars.len();
        let width = chars.first().map(|row| row.len()).unwrap_or(0);
        let styles = vec![vec![Style::default(); width]; height];
        Self { chars, styles }
    }

    pub fn new_with_style(chars: Vec<Vec<char>>, style: Style) -> Self {
        let height = chars.len();
        let width = chars.first().map(|row| row.len()).unwrap_or(0);
        let styles = vec![vec![style; width]; height];
        Self { chars, styles }
    }

    pub fn new_with_styles(chars: Vec<Vec<char>>, styles: Vec<Vec<Style>>) -> Self {
        Self { chars, styles }
    }

    pub fn single(ch: char, dims: CharDimensions) -> Self {
        let chars = vec![vec![ch; dims.horizontal]; dims.vertical];
        Self::new(chars)
    }

    pub fn single_with_style(ch: char, dims: CharDimensions, style: Style) -> Self {
        let chars = vec![vec![ch; dims.horizontal]; dims.vertical];
        Self::new_with_style(chars, style)
    }

    pub fn empty(dims: CharDimensions) -> Self {
        Self::single(' ', dims)
    }
}
