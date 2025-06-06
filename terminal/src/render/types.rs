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
        Self { horizontal, vertical }
    }
}

pub struct CharGrid {
    grid: Vec<Vec<char>>,
    logical_width: usize,
    logical_height: usize,
    char_dims: CharDimensions,
}

impl CharGrid {
    pub fn new(logical_width: usize, logical_height: usize, char_dims: CharDimensions) -> Self {
        let physical_width = logical_width * char_dims.horizontal;
        let physical_height = logical_height * char_dims.vertical;
        let grid = vec![vec![' '; physical_width]; physical_height];
        Self {
            grid,
            logical_width,
            logical_height,
            char_dims,
        }
    }

    pub fn set_logical_point(&mut self, x: usize, y: usize, pattern: &CharPattern) {
        let start_x = x * self.char_dims.horizontal;
        let start_y = y * self.char_dims.vertical;

        for (dy, row) in pattern.chars.iter().enumerate() {
            for (dx, &ch) in row.iter().enumerate() {
                if let Some(grid_row) = self.grid.get_mut(start_y + dy) {
                    if let Some(cell) = grid_row.get_mut(start_x + dx) {
                        *cell = ch;
                    }
                }
            }
        }
    }

    pub fn into_lines(self) -> Vec<Vec<char>> {
        self.grid
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
}

impl CharPattern {
    pub fn new(chars: Vec<Vec<char>>) -> Self {
        Self { chars }
    }

    pub fn single(ch: char, dims: CharDimensions) -> Self {
        let chars = vec![vec![ch; dims.horizontal]; dims.vertical];
        Self { chars }
    }

    pub fn empty(dims: CharDimensions) -> Self {
        Self::single(' ', dims)
    }
}