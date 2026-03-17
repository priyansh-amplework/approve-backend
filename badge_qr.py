"""
badge_qr.py - Advanced Badge Generation with Intelligent QR Code Management
FEATURES:
- Detects QR codes ANYWHERE in the image using CV2
- Removes all existing QR codes
- Places new tracking QR in optimal position (avoids text)
- Smart positioning algorithm
"""

from google import genai
from google.genai import types
from PIL import Image, ImageDraw, ImageFont
import io
import os
import random
import glob
import qrcode
import cv2
import numpy as np
from dotenv import load_dotenv

load_dotenv()

# Gemini API Configuration
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
gemini_client = genai.Client(api_key=GEMINI_API_KEY)

# Badge Configuration
BADGES_FOLDER = os.getenv("BADGES_FOLDER", "badges")
OUTPUT_FOLDER = os.getenv("OUTPUT_FOLDER", "generated_badges")

# Create folders if they don't exist
os.makedirs(BADGES_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# Default fallback URL
DEFAULT_URL = "https://nonai.life/"


# ============================================================================
# INTELLIGENT QR CODE DETECTION AND REMOVAL
# ============================================================================

def detect_all_qr_codes(image_path):
    """
    Detect ALL QR codes in the image regardless of position.
    Returns list of bounding boxes (x, y, w, h) for each detected QR.
    """
    img = cv2.imread(image_path)
    if img is None:
        print(f"   ⚠️ Could not load image: {image_path}")
        return []
    
    h, w, _ = img.shape
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    
    # High-frequency edge detection
    edges = cv2.Canny(gray, 100, 200)
    
    # Find contours
    contours, _ = cv2.findContours(edges, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    
    qr_boxes = []
    
    for cnt in contours:
        x, y, cw, ch = cv2.boundingRect(cnt)
        area = cw * ch
        
        # QR-like size constraints - WIDER RANGE for different QR sizes
        if 2000 < area < 100000:
            aspect_ratio = cw / float(ch)
            
            # Near-square (QR codes are square)
            if 0.7 < aspect_ratio < 1.4:
                roi = gray[y:y+ch, x:x+cw]
                contrast = roi.std()
                
                # QR-like contrast (high contrast black/white pattern)
                if contrast > 40:
                    qr_boxes.append({
                        'x': x,
                        'y': y,
                        'w': cw,
                        'h': ch,
                        'area': area,
                        'contrast': contrast
                    })
                    print(f"   🔍 QR detected at ({x}, {y}) size: {cw}x{ch}, contrast: {contrast:.1f}")
    
    return qr_boxes


def remove_all_qr_codes(image_path, output_path):
    """
    Remove ALL detected QR codes from the image using intelligent inpainting.
    Uses background sampling for natural removal.
    """
    img = cv2.imread(image_path)
    if img is None:
        print(f"   ⚠️ Could not load image: {image_path}")
        return image_path
    
    qr_boxes = detect_all_qr_codes(image_path)
    
    if not qr_boxes:
        print(f"   ℹ️ No QR codes detected in image")
        cv2.imwrite(output_path, img)
        return output_path
    
    print(f"   🧹 Removing {len(qr_boxes)} QR code(s)...")
    
    for qr in qr_boxes:
        x, y, w, h = qr['x'], qr['y'], qr['w'], qr['h']
        
        # Expand slightly to remove border artifacts
        margin = 5
        x1 = max(0, x - margin)
        y1 = max(0, y - margin)
        x2 = min(img.shape[1], x + w + margin)
        y2 = min(img.shape[0], y + h + margin)
        
        # Sample background color from surrounding area
        sample_x = max(x - 20, 0)
        sample_y = max(y - 20, 0)
        bg_color = img[sample_y, sample_x].tolist()
        
        # Fill with background color
        img[y1:y2, x1:x2] = bg_color
        
        # Optional: Add slight noise for more natural look
        noise = np.random.randint(-5, 5, (y2-y1, x2-x1, 3))
        img[y1:y2, x1:x2] = np.clip(img[y1:y2, x1:x2].astype(int) + noise, 0, 255).astype(np.uint8)
    
    cv2.imwrite(output_path, img)
    print(f"   ✅ All QR codes removed")
    
    return output_path


# ============================================================================
# TEXT DETECTION FOR SMART QR PLACEMENT
# ============================================================================

def detect_text_regions(image_path):
    """
    Detect text-heavy regions in the image to avoid placing QR over them.
    Returns list of rectangles to avoid.
    """
    img = cv2.imread(image_path)
    if img is None:
        return []
    
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    
    # Use morphological operations to detect text regions
    # Detect horizontal text structures
    kernel_horizontal = cv2.getStructuringElement(cv2.MORPH_RECT, (50, 2))
    horizontal = cv2.morphologyEx(gray, cv2.MORPH_CLOSE, kernel_horizontal)
    
    # Detect vertical text structures
    kernel_vertical = cv2.getStructuringElement(cv2.MORPH_RECT, (2, 50))
    vertical = cv2.morphologyEx(gray, cv2.MORPH_CLOSE, kernel_vertical)
    
    # Combine
    text_mask = cv2.bitwise_or(horizontal, vertical)
    
    # Threshold
    _, text_mask = cv2.threshold(text_mask, 200, 255, cv2.THRESH_BINARY)
    
    # Find text contours
    contours, _ = cv2.findContours(text_mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    
    text_regions = []
    for cnt in contours:
        x, y, w, h = cv2.boundingRect(cnt)
        if w > 50 and h > 20:  # Minimum text size
            text_regions.append({
                'x': x,
                'y': y,
                'w': w,
                'h': h
            })
    
    return text_regions


def find_optimal_qr_position(image_path, qr_size=180):
    """
    Find the best position for QR code that:
    1. Avoids text regions
    2. Avoids existing QR positions
    3. Prefers corners/edges
    
    Returns: (x, y, position_name)
    """
    img = cv2.imread(image_path)
    if img is None:
        return None, None, None
    
    h, w, _ = img.shape
    
    # Get existing QR positions to avoid
    existing_qrs = detect_all_qr_codes(image_path)
    
    # Get text regions to avoid
    text_regions = detect_text_regions(image_path)
    
    # Define candidate positions (corners and edges)
    margin = 30
    candidates = [
        {'x': w - qr_size - margin, 'y': h - qr_size - margin, 'name': 'bottom-right', 'priority': 1},
        {'x': margin, 'y': h - qr_size - margin, 'name': 'bottom-left', 'priority': 2},
        {'x': w - qr_size - margin, 'y': margin, 'name': 'top-right', 'priority': 3},
        {'x': margin, 'y': margin, 'name': 'top-left', 'priority': 4},
        {'x': w//2 - qr_size//2, 'y': h - qr_size - margin, 'name': 'bottom-center', 'priority': 5},
        {'x': w - qr_size - margin, 'y': h//2 - qr_size//2, 'name': 'middle-right', 'priority': 6},
    ]
    
    def check_overlap(x, y, w, h, regions):
        """Check if position overlaps with any region"""
        for region in regions:
            rx, ry, rw, rh = region['x'], region['y'], region['w'], region['h']
            
            # Check rectangle overlap
            if not (x + w < rx or x > rx + rw or y + h < ry or y > ry + rh):
                return True
        return False
    
    # Score each position
    best_pos = None
    best_score = -1
    
    for pos in sorted(candidates, key=lambda p: p['priority']):
        x, y = pos['x'], pos['y']
        
        # Skip if out of bounds
        if x < 0 or y < 0 or x + qr_size > w or y + qr_size > h:
            continue
        
        # Check overlaps
        overlaps_qr = check_overlap(x, y, qr_size, qr_size, existing_qrs)
        overlaps_text = check_overlap(x, y, qr_size, qr_size, text_regions)
        
        if not overlaps_qr and not overlaps_text:
            score = 10 - pos['priority']  # Higher priority = higher score
            
            if score > best_score:
                best_score = score
                best_pos = pos
    
    if best_pos:
        print(f"   📍 Optimal QR position: {best_pos['name']}")
        return best_pos['x'], best_pos['y'], best_pos['name']
    
    # Fallback: bottom-right corner
    print(f"   ⚠️ Using fallback position: bottom-right")
    return w - qr_size - margin, h - qr_size - margin, 'bottom-right'


# ============================================================================
# QR CODE GENERATION
# ============================================================================

def generate_qr_code(url, size=140):
    """
    Generate high-quality QR code with error correction.
    Returns PIL Image object.
    """
    qr = qrcode.QRCode(
        version=1,
        error_correction=qrcode.constants.ERROR_CORRECT_H,
        box_size=10,
        border=2,
    )
    qr.add_data(url)
    qr.make(fit=True)
    
    qr_img = qr.make_image(fill_color="black", back_color="white")
    qr_img = qr_img.resize((size, size), Image.Resampling.LANCZOS)
    
    return qr_img


def sample_background_color(img, x, y, size=15):
    """
    Sample background color from area around position.
    Returns average RGB color.
    """
    pixels = []
    w, h = img.size

    for dx in range(-size, size):
        for dy in range(-size, size):
            px = min(max(x + dx, 0), w - 1)
            py = min(max(y + dy, 0), h - 1)
            try:
                pixel = img.getpixel((px, py))
                if isinstance(pixel, tuple) and len(pixel) >= 3:
                    pixels.append(pixel[:3])
            except:
                continue
    
    if not pixels:
        return (15, 15, 15)
    
    return tuple(int(np.mean([p[i] for p in pixels])) for i in range(3))


def add_qr_code_to_image(image_path, qr_code, output_path, position=None):
    """
    Add QR code to image at optimal position with smart background blending.
    
    Args:
        image_path: Path to base image
        qr_code: PIL Image of QR code
        output_path: Where to save result
        position: Optional tuple (x, y) for manual position
    """
    img = Image.open(image_path).convert("RGBA")
    
    # QR card configuration
    qr_size = 140
    padding = 20
    card_size = qr_size + padding * 2
    
    # Resize QR if needed
    qr_code = qr_code.resize((qr_size, qr_size), Image.LANCZOS)
    
    # Find optimal position if not provided
    if position is None:
        pos_x, pos_y, pos_name = find_optimal_qr_position(image_path, card_size)
        if pos_x is None:
            # Fallback
            pos_x = img.width - card_size - 30
            pos_y = img.height - card_size - 30
    else:
        pos_x, pos_y = position
    
    # Sample background color for natural blending
    bg_color = sample_background_color(img, pos_x + card_size//2, pos_y + card_size//2)
    
    # Create QR card with sampled background
    card = Image.new("RGBA", (card_size, card_size), (*bg_color, 255))
    card.paste(qr_code, (padding, padding), qr_code)
    
    # Add subtle border
    draw = ImageDraw.Draw(card)
    border_color = tuple(max(0, min(255, c - 30)) for c in bg_color)
    draw.rectangle([0, 0, card_size-1, card_size-1], outline=border_color, width=2)
    
    # Paste card with alpha blending
    img.paste(card, (pos_x, pos_y), card)
    
    # Convert to RGB and save
    img.convert("RGB").save(output_path, "JPEG", quality=95, optimize=True)
    
    print(f"   📱 QR code added successfully")
    
    return output_path


# ============================================================================
# TEMPLATE MANAGEMENT
# ============================================================================

def get_platform_templates(platform):
    """Get all template images for a specific platform."""
    platform_lower = platform.lower()
    
    patterns = [
        os.path.join(BADGES_FOLDER, f"{platform_lower}_*.png"),
        os.path.join(BADGES_FOLDER, f"{platform_lower}_*.jpg"),
        os.path.join(BADGES_FOLDER, f"{platform_lower}*.png"),
        os.path.join(BADGES_FOLDER, f"{platform_lower}*.jpg"),
    ]
    
    templates = []
    for pattern in patterns:
        templates.extend(glob.glob(pattern))
    
    templates = sorted(list(set(templates)))
    return templates


def select_random_template(platform):
    """Randomly select a template for the given platform."""
    templates = get_platform_templates(platform)
    
    if not templates:
        print(f"   ⚠️ No templates found for platform: {platform}")
        return None
    
    selected = random.choice(templates)
    template_name = os.path.basename(selected)
    
    print(f"   🎲 Selected template: {template_name}")
    
    return selected


def list_available_templates():
    """List all available templates organized by platform."""
    all_files = glob.glob(os.path.join(BADGES_FOLDER, "*.*"))
    templates_by_platform = {}
    
    for filepath in all_files:
        filename = os.path.basename(filepath)
        
        if not filename.lower().endswith(('.png', '.jpg', '.jpeg')):
            continue
        
        platform = filename.split('_')[0].split('.')[0]
        platform = ''.join([c for c in platform if not c.isdigit()])
        
        if platform not in templates_by_platform:
            templates_by_platform[platform] = []
        
        templates_by_platform[platform].append(filepath)
    
    return templates_by_platform


# ============================================================================
# MAIN BADGE GENERATION WITH INTELLIGENT QR MANAGEMENT
# ============================================================================

def generate_personalized_badge(
    name, 
    platform="facebook", 
    template_path=None, 
    output_path=None,
    tracking_url=None,
    qr_position=None,
    remove_existing_qr=True
):
    """
    Generate a personalized badge with intelligent QR code management.
    
    ADVANCED FEATURES:
    - Detects QR codes ANYWHERE in the image
    - Removes all existing QR codes intelligently
    - Places new tracking QR in optimal position (avoids text)
    - Smart background color matching
    
    Args:
        name (str): The name to add to the badge
        platform (str): Platform name for template selection
        template_path (str, optional): Specific template path
        output_path (str, optional): Path where generated image will be saved
        tracking_url (str, optional): URL for QR code (defaults to nonai.life)
        qr_position (tuple, optional): Manual (x, y) position, or None for auto
        remove_existing_qr (bool): Whether to detect and remove existing QR codes
    
    Returns:
        tuple: (success: bool, output_path: str or None, error_message: str or None)
    """
    
    # Auto-select template if not provided
    if template_path is None:
        template_path = select_random_template(platform)
        if template_path is None:
            error_msg = f"No templates found for platform: {platform}"
            return False, None, error_msg
    
    # Auto-generate output path if not provided
    if output_path is None:
        import time
        timestamp = int(time.time())
        safe_name = name.replace(' ', '_').replace('/', '_').replace('\\', '_')
        template_name = os.path.basename(template_path).split('.')[0]
        output_filename = f"{safe_name}_{template_name}_{timestamp}.jpg"
        output_path = os.path.join(OUTPUT_FOLDER, output_filename)
    
    # Check if template exists
    if not os.path.exists(template_path):
        error_msg = f"Template not found: {template_path}"
        return False, None, error_msg
    
    # Check if Gemini API key is configured
    if not GEMINI_API_KEY:
        error_msg = "GEMINI_API_KEY not found in environment variables"
        return False, None, error_msg
    
    try:
        print(f"\n{'='*70}")
        print(f"🎨 GENERATING BADGE FOR: {name}")
        print(f"{'='*70}")
        
        # ===== STEP 1: Generate personalized badge with Gemini =====
        print(f"📝 Step 1: Personalizing badge with Gemini...")
        
        with open(template_path, "rb") as f:
            image_bytes = f.read()
        
        mime_type = "image/png"
        if template_path.lower().endswith(('.jpg', '.jpeg')):
            mime_type = "image/jpeg"
        
        prompt = (
            f"IMPORTANT: Search the ENTIRE image carefully and replace EVERY single occurrence of 'James' with '{name}'.\n"
            f"This includes text in buttons, ribbons, badges, headers, and any other location.\n"
            f"Count: Replace James if it appears 1 time, 2 times, 3 times, or more - replace ALL of them.\n"
            f"For each replacement:\n"
            f"- Match the exact font style, size, color, and alignment\n"
            f"- Remove any quotation marks\n"
            f"- Keep all other design elements unchanged\n"
            f"- PRESERVE any existing QR codes exactly as they are (DO NOT modify QR codes)\n"
            f"Double-check that no instance of 'James' remains in the final image."
        )
        
        response = gemini_client.models.generate_content(
            model="gemini-2.5-flash-image",
            contents=[
                types.Part.from_bytes(data=image_bytes, mime_type=mime_type),
                prompt
            ],
            config=types.GenerateContentConfig(
                response_modalities=["IMAGE"]
            )
        )
        
        # Save the generated image to temporary path
        temp_path = output_path.replace('.jpg', '_temp.jpg').replace('.png', '_temp.png')
        
        image_saved = False
        for part in response.candidates[0].content.parts:
            if part.inline_data:
                final_img = Image.open(io.BytesIO(part.inline_data.data))
                os.makedirs(os.path.dirname(temp_path) or '.', exist_ok=True)
                final_img.save(temp_path)
                image_saved = True
                break
        
        if not image_saved:
            error_msg = "No image data in Gemini response"
            return False, None, error_msg
        
        print(f"   ✅ Badge personalized with name '{name}'")
        
        # ===== STEP 2: Detect and remove ALL existing QR codes =====
        if remove_existing_qr:
            print(f"\n🔍 Step 2: Detecting and removing existing QR codes...")
            qr_removed_path = output_path.replace('.jpg', '_noqr.jpg').replace('.png', '_noqr.png')
            remove_all_qr_codes(temp_path, qr_removed_path)
            temp_path = qr_removed_path
        
        # ===== STEP 3: Add new tracking QR code in optimal position =====
        qr_url = tracking_url or DEFAULT_URL
        print(f"\n📱 Step 3: Adding tracking QR code...")
        print(f"   🔗 URL: {qr_url[:60]}...")
        
        qr_code = generate_qr_code(qr_url, size=140)
        add_qr_code_to_image(temp_path, qr_code, output_path, position=qr_position)
        
        # Clean up temporary files
        try:
            if os.path.exists(temp_path):
                os.remove(temp_path)
            qr_removed_path = output_path.replace('.jpg', '_noqr.jpg').replace('.png', '_noqr.png')
            if os.path.exists(qr_removed_path):
                os.remove(qr_removed_path)
        except:
            pass
        
        print(f"\n✅ BADGE GENERATION COMPLETE!")
        print(f"   📁 Output: {os.path.basename(output_path)}")
        print(f"{'='*70}\n")
        
        return True, output_path, None
        
    except FileNotFoundError as e:
        error_msg = f"File error: {str(e)}"
        return False, None, error_msg
    
    except Exception as e:
        error_msg = f"Badge generation failed: {str(e)}"
        return False, None, error_msg


def get_badge_info():
    """Get information about badge generation configuration."""
    templates_by_platform = list_available_templates()
    total_templates = sum(len(templates) for templates in templates_by_platform.values())
    
    return {
        'gemini_api_configured': bool(GEMINI_API_KEY),
        'badges_folder': BADGES_FOLDER,
        'output_folder': OUTPUT_FOLDER,
        'templates_by_platform': templates_by_platform,
        'total_templates': total_templates,
        'platforms': list(templates_by_platform.keys()),
        'output_folder_exists': os.path.exists(OUTPUT_FOLDER)
    }


# ============================================================================
# TESTING
# ============================================================================

def test_badge_generation():
    """Test intelligent badge generation with QR detection and optimal placement."""
    
    print("\n" + "="*70)
    print("🧪 TESTING INTELLIGENT QR-ENHANCED BADGE GENERATION")
    print("="*70)
    print("   ✓ Detects QR codes anywhere in image")
    print("   ✓ Removes all existing QR codes")
    print("   ✓ Places new QR in optimal position")
    print("   ✓ Avoids covering text")
    print("="*70 + "\n")
    
    info = get_badge_info()
    print("📋 Configuration:")
    print(f"   Gemini API: {'✅ Configured' if info['gemini_api_configured'] else '❌ Not configured'}")
    print(f"   Badges Folder: {info['badges_folder']}")
    print(f"   Output Folder: {info['output_folder']}")
    print(f"   Total Templates: {info['total_templates']}")
    print()
    
    if not info['gemini_api_configured']:
        print("❌ GEMINI_API_KEY not found in environment")
        return
    
    if info['total_templates'] == 0:
        print(f"❌ No templates found in {info['badges_folder']}/")
        return
    
    print("📁 Available Templates:")
    for platform, templates in info['templates_by_platform'].items():
        print(f"\n   {platform.upper()} ({len(templates)} templates):")
        for template in templates[:3]:  # Show first 3
            print(f"      • {os.path.basename(template)}")
    
    print("\n" + "="*70)
    
    # Test cases
    test_cases = [
        {
            "name": "Sarah Chen",
            "platform": "facebook",
            "tracking_url": "https://nonai.life/track/test001",
        },
    ]
    
    for idx, test in enumerate(test_cases, 1):
        print(f"\n🎯 Test {idx}: {test['name']} ({test['platform']})")
        print(f"   Tracking: {test['tracking_url']}")
        
        success, output_path, error = generate_personalized_badge(
            name=test['name'],
            platform=test['platform'],
            tracking_url=test['tracking_url'],
            remove_existing_qr=True
        )
        
        if success:
            print(f"\n✅ SUCCESS!")
            print(f"   📁 File: {output_path}")
            file_size = os.path.getsize(output_path)
            print(f"   📊 Size: {file_size / 1024:.2f} KB")
        else:
            print(f"\n❌ FAILED: {error}")
        
        print("="*70)
        
        import time
        time.sleep(2)


if __name__ == "__main__":
    test_badge_generation()