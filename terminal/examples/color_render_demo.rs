use common::{Arena, Snake, Position, Direction};
use terminal::render::{
    arena::ArenaRenderer,
    standard_renderer::StandardRenderer,
    types::{CharDimensions, RenderConfig},
};

fn main() {
    // Create a demo arena with multiple snakes
    let arena = Arena {
        width: 20,
        height: 10,
        snakes: vec![
            Snake {
                body: vec![
                    Position { x: 5, y: 5 },   // head
                    Position { x: 5, y: 7 },   // turn point
                    Position { x: 3, y: 7 },   // tail
                ],
                direction: Direction::Up,
                is_alive: true,
                food: 0,
            },
            Snake {
                body: vec![
                    Position { x: 15, y: 3 },  // head
                    Position { x: 12, y: 3 },  // tail
                ],
                direction: Direction::Right,
                is_alive: true,
                food: 0,
            },
            Snake {
                body: vec![
                    Position { x: 8, y: 8 },   // head
                    Position { x: 8, y: 6 },   // tail
                ],
                direction: Direction::Down,
                is_alive: true,
                food: 0,
            },
            Snake {
                body: vec![
                    Position { x: 17, y: 7 },  // head
                    Position { x: 17, y: 5 },  // tail
                ],
                direction: Direction::Down,
                is_alive: true,
                food: 0,
            },
        ],
        food: vec![
            Position { x: 10, y: 5 },
            Position { x: 2, y: 2 },
            Position { x: 17, y: 8 },
        ],
    };

    println!("=== Color-based Snake Rendering Demo ===\n");
    
    println!("Snake Colors:");
    println!("  Player 0: White (head) → Gray (body)");
    println!("  Player 1: Light Green (head) → Green (body)");
    println!("  Player 2: Light Blue (head) → Blue (body)");
    println!("  Player 3: Light Yellow (head) → Yellow (body)");
    println!("  Food: Red\n");

    println!("=== 2x1 Rendering ===");
    render_with_dimensions(&arena, CharDimensions::new(2, 1));
}

fn render_with_dimensions(arena: &Arena, char_dims: CharDimensions) {
    let renderer = StandardRenderer::new(char_dims);
    let arena_renderer = ArenaRenderer::new(renderer);
    let config = RenderConfig { chars_per_point: char_dims };
    
    let char_grid = arena_renderer.render(arena, &config);
    let styled_lines = char_grid.into_styled_lines();
    
    println!("Physical size: {}x{} characters", 
        arena.width as usize * char_dims.horizontal,
        arena.height as usize * char_dims.vertical
    );
    
    // Draw top border
    print!("┌");
    for _ in 0..arena.width as usize * char_dims.horizontal {
        print!("─");
    }
    println!("┐");
    
    // Draw arena with side borders
    // Note: In a real terminal with color support, these would show in their respective colors
    for (chars, styles) in styled_lines {
        print!("│");
        for (ch, _style) in chars.into_iter().zip(styles.into_iter()) {
            print!("{}", ch);
        }
        println!("│");
    }
    
    // Draw bottom border
    print!("└");
    for _ in 0..arena.width as usize * char_dims.horizontal {
        print!("─");
    }
    println!("┘");
    
    println!("\nNote: Run this in a terminal application to see actual colors!");
}