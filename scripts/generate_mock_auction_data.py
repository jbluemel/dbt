import psycopg2
from datetime import datetime, timedelta
import random
from decimal import Decimal

# Database connection
conn = psycopg2.connect(
    host="172.26.5.215",
    port=5434,
    database="dbt_dev",
    user="dbt_user",
    password="dbt_password"
)
cur = conn.cursor()

print("Generating realistic Purple Wave auction data...")
print("=" * 60)

# Category definitions with realistic pricing
CATEGORIES = {
    'Construction': {
        'models': ['D9', 'PC490', '624K', 'EC220', '950M', 'ZX350', '850M'],
        'price_range': (25000, 50000),
        'volume_pct': 0.20  # 20% of items
    },
    'Ag Equipment': {
        'models': ['8735', 'S780', 'M7-172', 'CR8.90', 'DB60', '9250'],
        'price_range': (15000, 35000),
        'volume_pct': 0.25  # 25% of items
    },
    'Truck/Trailer': {
        'models': ['T680', 'T880', '567', 'W900', 'Cascadia', 'VNL'],
        'price_range': (8000, 20000),
        'volume_pct': 0.35  # 35% of items
    },
    'Passenger': {
        'models': ['F-150', 'Silverado', 'Ram 1500', 'Accord', 'Camry', 'Civic'],
        'price_range': (2000, 8000),
        'volume_pct': 0.20  # 20% of items
    }
}

# Fee structure
def generate_fees(hammer_price):
    """Generate realistic fees based on hammer price"""
    seller_service_fee = 200
    lot_fee = 150
    power_washing = 225 if random.random() > 0.6 else 0
    decal_removal = 100 if random.random() > 0.8 else 0
    
    total_fees = seller_service_fee + lot_fee + power_washing + decal_removal
    contract_price = hammer_price + (hammer_price * 0.10)  # 10% buyer premium
    
    return {
        'seller_service_fee': seller_service_fee,
        'lot_fee': lot_fee,
        'power_washing': power_washing,
        'decal_removal': decal_removal,
        'total_fees': total_fees,
        'contract_price': contract_price
    }

# Generate dates: Aug-Dec, Tues/Wed/Thurs only
def generate_auction_dates():
    """Generate auction dates from August to December 2025"""
    dates = []
    start_date = datetime(2025, 8, 1)
    end_date = datetime(2025, 12, 31)
    
    current = start_date
    while current <= end_date:
        # 1=Monday, 2=Tuesday, 3=Wednesday, 4=Thursday
        if current.weekday() in [1, 2, 3]:  # Tues, Wed, Thurs
            dates.append(current)
        current += timedelta(days=1)
    
    return dates

# Volume by month
def items_per_day(date):
    """Determine how many items for this day"""
    month = date.month
    
    if month == 12:  # December: 3-4x normal
        return random.randint(600, 1200)
    else:  # August-November: normal
        return random.randint(200, 300)

# Category mix variations to create interesting weeks
def get_category_mix(week_number):
    """Adjust category mix for different scenarios"""
    
    # Week 10 (late October): Dip week - more passenger vehicles
    if week_number == 10:
        return {
            'Construction': 0.15,  # -5%
            'Ag Equipment': 0.20,  # -5%
            'Truck/Trailer': 0.35,  # same
            'Passenger': 0.30      # +10% (more low-value items)
        }
    
    # Week 15 (late November): Pre-holiday slowdown
    elif week_number == 15:
        return {
            'Construction': 0.18,  # -2%
            'Ag Equipment': 0.22,  # -3%
            'Truck/Trailer': 0.38,  # +3%
            'Passenger': 0.22      # +2%
        }
    
    # Normal mix
    else:
        return {
            'Construction': 0.20,
            'Ag Equipment': 0.25,
            'Truck/Trailer': 0.35,
            'Passenger': 0.20
        }

# Clear existing data
print("\n1. Clearing existing data...")
cur.execute("DELETE FROM itemsbasics")
conn.commit()
print("   ✓ Cleared")

# Generate items
print("\n2. Generating auction items...")
dates = generate_auction_dates()
item_id = 1
week_number = 0
current_week_start = None

for auction_date in dates:
    # Track week number
    if current_week_start is None or (auction_date - current_week_start).days >= 7:
        week_number += 1
        current_week_start = auction_date
    
    # Get category mix for this week
    mix = get_category_mix(week_number)
    
    # Number of items for this day
    num_items = items_per_day(auction_date)
    
    # Generate items
    for _ in range(num_items):
        # Select category based on mix
        rand = random.random()
        cumulative = 0
        selected_category = None
        
        for category, pct in mix.items():
            cumulative += pct
            if rand <= cumulative:
                selected_category = category
                break
        
        if not selected_category:
            selected_category = 'Passenger'  # fallback
        
        # Generate item
        cat_info = CATEGORIES[selected_category]
        model = random.choice(cat_info['models'])
        hammer_price = random.randint(cat_info['price_range'][0], cat_info['price_range'][1])
        fees = generate_fees(hammer_price)
        icn = f"{random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ')}{random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ')}{random.randint(1000,9999)}"
        
        # Insert item
        cur.execute("""
            INSERT INTO itemsbasics 
            (unique_id, auctiondate, icn, model, category, hammer, contract_price,
             seller_service_fee, lot_fee, power_washing, decal_removal, total_fees)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            str(item_id),
            auction_date,
            icn,
            model,
            selected_category,
            hammer_price,
            fees['contract_price'],
            fees['seller_service_fee'],
            fees['lot_fee'],
            fees['power_washing'],
            fees['decal_removal'],
            fees['total_fees']
        ))
        
        item_id += 1
        
        if item_id % 1000 == 0:
            print(f"   Generated {item_id} items...")
            conn.commit()

conn.commit()
print(f"   ✓ Generated {item_id - 1} total items")

# Summary
print("\n3. Data Summary:")
cur.execute("""
    SELECT 
        category,
        COUNT(*) as count,
        AVG(hammer) as avg_price,
        MIN(auctiondate) as first_date,
        MAX(auctiondate) as last_date
    FROM itemsbasics
    GROUP BY category
    ORDER BY category
""")

for row in cur.fetchall():
    print(f"   {row[0]}: {row[1]} items, avg ${row[2]:,.0f} ({row[3].strftime('%Y-%m-%d')} to {row[4].strftime('%Y-%m-%d')})")

# Overall average
cur.execute("SELECT AVG(hammer), COUNT(*) FROM itemsbasics")
avg, total = cur.fetchone()
print(f"\n   Overall: {total} items, avg lot value ${avg:,.0f}")

cur.close()
conn.close()

print("\n" + "=" * 60)
print("✓ Data generation complete!")
