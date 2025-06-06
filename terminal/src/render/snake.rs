use common::{Snake, Position};

pub struct SnakeRenderer;

impl SnakeRenderer {
    /// Expand compressed snake representation into full body positions
    pub fn expand_snake_body(snake: &Snake) -> Vec<Position> {
        if snake.body.len() < 2 {
            return snake.body.clone();
        }
        
        let mut positions = Vec::new();
        
        // Process each segment between consecutive body points
        for window in snake.body.windows(2) {
            let start = &window[0];
            let end = &window[1];
            
            // Generate all positions between start and end
            if start.x == end.x {
                // Vertical segment
                let _min_y = start.y.min(end.y);
                let _max_y = start.y.max(end.y);
                
                if positions.is_empty() || positions.last() != Some(start) {
                    // Add all points from start to end
                    if start.y <= end.y {
                        for y in start.y..=end.y {
                            positions.push(Position { x: start.x, y });
                        }
                    } else {
                        for y in (end.y..=start.y).rev() {
                            positions.push(Position { x: start.x, y });
                        }
                    }
                } else {
                    // Skip the first point as it's already in positions
                    if start.y <= end.y {
                        for y in (start.y + 1)..=end.y {
                            positions.push(Position { x: start.x, y });
                        }
                    } else {
                        for y in (end.y..start.y).rev() {
                            positions.push(Position { x: start.x, y });
                        }
                    }
                }
            } else if start.y == end.y {
                // Horizontal segment
                if positions.is_empty() || positions.last() != Some(start) {
                    // Add all points from start to end
                    if start.x <= end.x {
                        for x in start.x..=end.x {
                            positions.push(Position { x, y: start.y });
                        }
                    } else {
                        for x in (end.x..=start.x).rev() {
                            positions.push(Position { x, y: start.y });
                        }
                    }
                } else {
                    // Skip the first point as it's already in positions
                    if start.x <= end.x {
                        for x in (start.x + 1)..=end.x {
                            positions.push(Position { x, y: start.y });
                        }
                    } else {
                        for x in (end.x..start.x).rev() {
                            positions.push(Position { x, y: start.y });
                        }
                    }
                }
            } else {
                // This shouldn't happen with valid snake data
                // Just add the end point
                if positions.is_empty() || positions.last() != Some(start) {
                    positions.push(*start);
                }
                positions.push(*end);
            }
        }
        
        // Remove any duplicates that might have been created
        let mut deduped = Vec::new();
        for pos in positions {
            if deduped.is_empty() || deduped.last() != Some(&pos) {
                deduped.push(pos);
            }
        }
        
        deduped
    }
}