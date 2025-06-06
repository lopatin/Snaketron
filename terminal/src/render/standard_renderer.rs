use common::Direction;
use super::traits::GameObjectRenderer;
use super::types::{CharPattern, CharDimensions};

pub struct StandardRenderer {
    char_dims: CharDimensions,
}

impl StandardRenderer {
    pub fn new(char_dims: CharDimensions) -> Self {
        Self { char_dims }
    }
}

impl GameObjectRenderer for StandardRenderer {
    fn char_dimensions(&self) -> CharDimensions {
        self.char_dims
    }
    
    fn render_snake_segment(
        &self,
        _direction: Option<Direction>,
        is_head: bool,
        player_id: u32,
    ) -> CharPattern {
        let chars = if self.char_dims.horizontal == 2 && self.char_dims.vertical == 1 {
            // 2x1 rendering - use brightness to distinguish head from body
            if is_head {
                // Bright white for head
                vec![vec!['█', '█']]
            } else {
                // Different shades for different players
                let body_char = match player_id % 4 {
                    0 => '▓',  // Slightly darker than head
                    1 => '▒',  // Medium shade
                    2 => '░',  // Light shade
                    _ => '▒',  // Default to medium
                };
                vec![vec![body_char, body_char]]
            }
        } else if self.char_dims.horizontal == 1 && self.char_dims.vertical == 1 {
            // 1x1 rendering (classic mode)
            let char = if is_head {
                // Bright white for head
                '█'
            } else {
                // Different shades for different players
                match player_id % 4 {
                    0 => '▓',
                    1 => '▒',
                    2 => '░',
                    _ => '▒',
                }
            };
            vec![vec![char]]
        } else {
            // Fallback for other dimensions
            let fill_char = if is_head { '█' } else {
                match player_id % 4 {
                    0 => '▓',
                    1 => '▒',
                    2 => '░',
                    _ => '▒',
                }
            };
            vec![vec![fill_char; self.char_dims.horizontal]; self.char_dims.vertical]
        };
        
        CharPattern::new(chars)
    }
    
    fn render_food(&self) -> CharPattern {
        let chars = if self.char_dims.horizontal == 2 && self.char_dims.vertical == 1 {
            vec![vec!['●', '●']]
        } else if self.char_dims.horizontal == 1 && self.char_dims.vertical == 1 {
            vec![vec!['●']]
        } else {
            // For larger dimensions, create a pattern with food char in center-ish positions
            let mut pattern = vec![vec![' '; self.char_dims.horizontal]; self.char_dims.vertical];
            
            // Place food characters in a reasonable pattern
            for y in 0..self.char_dims.vertical {
                for x in 0..self.char_dims.horizontal {
                    // Create a checkerboard-like pattern for larger sizes
                    if (x + y) % 2 == 0 {
                        pattern[y][x] = '●';
                    }
                }
            }
            pattern
        };
        
        CharPattern::new(chars)
    }
    
    fn render_empty(&self) -> CharPattern {
        CharPattern::empty(self.char_dims)
    }
}