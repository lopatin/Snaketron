use common::{Arena, Snake, Position, Direction};
use terminal::render::{
    arena::ArenaRenderer,
    standard_renderer::StandardRenderer,
    types::{CharDimensions, RenderConfig},
};

#[test]
fn test_2x1_rendering() {
    // Create a simple arena with one snake and one food
    let mut arena = Arena {
        width: 10,
        height: 10,
        snakes: vec![
            Snake {
                body: vec![
                    Position { x: 5, y: 5 },  // head
                    Position { x: 4, y: 5 },  // turn/tail
                ],
                direction: Direction::Right,
                is_alive: true,
                food: 0,
            }
        ],
        food: vec![Position { x: 7, y: 7 }],
    };

    // Create renderer with 2x1 configuration
    let char_dims = CharDimensions::new(2, 1);
    let renderer = StandardRenderer::new(char_dims);
    let arena_renderer = ArenaRenderer::new(renderer);
    let config = RenderConfig { chars_per_point: char_dims };

    // Render the arena
    let char_grid = arena_renderer.render(&arena, &config);
    let lines = char_grid.into_lines();

    // Verify dimensions
    assert_eq!(lines.len(), 10);  // height remains same
    assert_eq!(lines[0].len(), 20); // width doubled (10 * 2)

    // Verify snake head is rendered (at position 5,5 -> chars 10,11 at row 5)
    assert_eq!(lines[5][10], '█');
    assert_eq!(lines[5][11], '█');

    // Verify snake body is rendered (at position 4,5 -> chars 8,9 at row 5)
    assert_eq!(lines[5][8], '█');
    assert_eq!(lines[5][9], '█');

    // Verify food is rendered (at position 7,7 -> chars 14,15 at row 7)
    assert_eq!(lines[7][14], '●');
    assert_eq!(lines[7][15], '●');
}

#[test]
fn test_1x1_rendering() {
    // Create a simple arena
    let arena = Arena {
        width: 5,
        height: 5,
        snakes: vec![
            Snake {
                body: vec![
                    Position { x: 2, y: 2 },
                    Position { x: 1, y: 2 },
                ],
                direction: Direction::Right,
                is_alive: true,
                food: 0,
            }
        ],
        food: vec![Position { x: 3, y: 3 }],
    };

    // Create renderer with 1x1 configuration
    let char_dims = CharDimensions::new(1, 1);
    let renderer = StandardRenderer::new(char_dims);
    let arena_renderer = ArenaRenderer::new(renderer);
    let config = RenderConfig { chars_per_point: char_dims };

    // Render the arena
    let char_grid = arena_renderer.render(&arena, &config);
    let lines = char_grid.into_lines();

    // Verify dimensions
    assert_eq!(lines.len(), 5);
    assert_eq!(lines[0].len(), 5);

    // Verify snake head
    assert_eq!(lines[2][2], '█');

    // Verify snake body
    assert_eq!(lines[2][1], '█');

    // Verify food
    assert_eq!(lines[3][3], '●');
}

#[test]
fn test_custom_dimensions() {
    // Test with 3x2 character dimensions
    let arena = Arena {
        width: 3,
        height: 3,
        snakes: vec![],
        food: vec![Position { x: 1, y: 1 }],
    };

    let char_dims = CharDimensions::new(3, 2);
    let renderer = StandardRenderer::new(char_dims);
    let arena_renderer = ArenaRenderer::new(renderer);
    let config = RenderConfig { chars_per_point: char_dims };

    let char_grid = arena_renderer.render(&arena, &config);
    let lines = char_grid.into_lines();

    // Verify dimensions
    assert_eq!(lines.len(), 6);  // 3 * 2
    assert_eq!(lines[0].len(), 9); // 3 * 3

    // Food should be rendered with checkerboard pattern at position (1,1)
    // Which maps to chars 3-5 on rows 2-3
    assert_eq!(lines[2][3], '●'); // (0,0) in pattern
    assert_eq!(lines[2][4], ' '); // (1,0) in pattern
    assert_eq!(lines[2][5], '●'); // (2,0) in pattern
    assert_eq!(lines[3][3], ' '); // (0,1) in pattern
    assert_eq!(lines[3][4], '●'); // (1,1) in pattern
    assert_eq!(lines[3][5], ' '); // (2,1) in pattern
}