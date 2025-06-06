use common::{Snake, Position, Direction};
use terminal::render::snake::SnakeRenderer;

#[test]
fn test_snake_expansion_scenarios() {
    // Test case 1: Snake moving left (like in the replay)
    let snake1 = Snake {
        body: vec![Position { x: 36, y: 20 }, Position { x: 38, y: 20 }],
        direction: Direction::Left,
        is_alive: true,
        food: 0,
    };
    
    println!("Snake 1 (moving left):");
    println!("  Compressed: {:?}", snake1.body);
    let expanded1 = SnakeRenderer::expand_snake_body(&snake1);
    println!("  Expanded: {:?}", expanded1);
    println!("  Length: {}", expanded1.len());
    assert_eq!(expanded1.len(), 3);
    
    // Test case 2: After several steps forward
    let mut snake2 = snake1.clone();
    for i in 1..=10 {
        snake2.step_forward();
        let expanded = SnakeRenderer::expand_snake_body(&snake2);
        println!("\n  After {} steps:", i);
        println!("    Compressed: {:?}", snake2.body);
        println!("    Expanded length: {}", expanded.len());
        if expanded.len() < 3 {
            println!("    WARNING: Snake shrunk to {} positions!", expanded.len());
            println!("    Expanded: {:?}", expanded);
        }
    }
    
    // Test case 3: Snake at edge
    let snake3 = Snake {
        body: vec![Position { x: 1, y: 20 }, Position { x: 3, y: 20 }],
        direction: Direction::Left,
        is_alive: true,
        food: 0,
    };
    
    println!("\n\nSnake 3 (near left edge):");
    println!("  Compressed: {:?}", snake3.body);
    let expanded3 = SnakeRenderer::expand_snake_body(&snake3);
    println!("  Expanded: {:?}", expanded3);
    println!("  Length: {}", expanded3.len());
    assert_eq!(expanded3.len(), 3);
    
    let mut snake4 = snake3.clone();
    snake4.step_forward();
    println!("\n  After 1 step:");
    println!("    Compressed: {:?}", snake4.body);
    let expanded4 = SnakeRenderer::expand_snake_body(&snake4);
    println!("    Expanded: {:?}", expanded4);
    println!("    Length: {}", expanded4.len());
}