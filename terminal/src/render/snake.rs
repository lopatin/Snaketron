use common::{Position, Snake};

pub struct SnakeRenderer;

impl SnakeRenderer {
    /// Expand compressed snake representation into full body positions
    pub fn expand_snake_body(snake: &Snake) -> Vec<Position> {
        if snake.body.len() < 2 {
            return snake.body.clone();
        }

        let mut positions = Vec::new();

        // Process each segment between consecutive body points
        for (i, window) in snake.body.windows(2).enumerate() {
            let start = &window[0];
            let end = &window[1];

            // For the first segment, always include the start point
            // For subsequent segments, skip the start point (it's already added as the end of previous segment)
            let skip_start = i > 0;

            // Generate all positions between start and end
            if start.x == end.x {
                // Vertical segment
                if start.y < end.y {
                    // Going down
                    let begin = if skip_start { start.y + 1 } else { start.y };
                    for y in begin..=end.y {
                        positions.push(Position { x: start.x, y });
                    }
                } else {
                    // Going up
                    let begin = if skip_start { start.y - 1 } else { start.y };
                    for y in (end.y..=begin).rev() {
                        positions.push(Position { x: start.x, y });
                    }
                }
            } else if start.y == end.y {
                // Horizontal segment
                if start.x < end.x {
                    // Going right
                    let begin = if skip_start { start.x + 1 } else { start.x };
                    for x in begin..=end.x {
                        positions.push(Position { x, y: start.y });
                    }
                } else {
                    // Going left
                    let begin = if skip_start { start.x - 1 } else { start.x };
                    for x in (end.x..=begin).rev() {
                        positions.push(Position { x, y: start.y });
                    }
                }
            } else {
                // This shouldn't happen with valid snake data (diagonal segment)
                // Just add both points
                if !skip_start {
                    positions.push(*start);
                }
                positions.push(*end);
            }
        }

        positions
    }
}
