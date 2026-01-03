#!/usr/bin/env python3
"""
Generate realistic Purple Wave auction data for Bronze layer tables.

Creates 4 CSV files:
- customers.csv: Buyers and sellers with geographic distribution
- items_v2.csv: 25,000 auction items (Aug-Dec 2025)
- bids.csv: Bid history showing auction activity
- fees.csv: Fee records for all items

Geographic Distribution:
- 70% Midwest (KS/MO/OK heaviest)
- 15% East Coast
- 15% West Coast

Temporal Distribution:
- Aug-Nov: ~15,000 items (~313/day, 3 days/week)
- December: ~10,000 items (~833/day, 3 days/week)
- Week 10 dip: Lower avg lot value ($8k-$8.5k)
- Week 15 slowdown: Reduced volume (Thanksgiving week)
"""

import csv
import random
from datetime import datetime, timedelta
from collections import defaultdict

# Set random seed for reproducibility
random.seed(42)

# ============================================================================
# CONFIGURATION
# ============================================================================

# Category distribution (normal weeks)
CATEGORY_DISTRIBUTION = {
    'Construction': 0.20,      # $25k-$50k
    'Ag Equipment': 0.25,      # $15k-$35k
    'Truck/Trailer': 0.35,     # $8k-$20k
    'Passenger': 0.20          # $2k-$8k
}

# Week 10 special (dip week) - more passenger vehicles
WEEK_10_DISTRIBUTION = {
    'Construction': 0.15,
    'Ag Equipment': 0.25,
    'Truck/Trailer': 0.30,
    'Passenger': 0.30          # Increased from 20%
}

# Price ranges by category
PRICE_RANGES = {
    'Construction': (25000, 50000),
    'Ag Equipment': (15000, 35000),
    'Truck/Trailer': (8000, 20000),
    'Passenger': (2000, 8000)
}

# Geographic distribution (70% Midwest, 15% East, 15% West)
STATE_DISTRIBUTION = {
    # Midwest (70%) - Top tier
    'KS': 3500,  # Home base
    'MO': 3000,
    'OK': 2500,
    'TX': 2000,
    # Midwest - Medium tier
    'NE': 800,
    'IA': 800,
    'IL': 800,
    'IN': 700,
    'OH': 700,
    # Midwest - Lower tier
    'MI': 500,
    'WI': 500,
    'MN': 500,
    'ND': 200,
    'SD': 200,
    # East Coast (15%)
    'NC': 800,
    'GA': 800,
    'FL': 800,
    'PA': 400,
    'VA': 400,
    'SC': 350,
    'NY': 200,
    # West Coast (15%)
    'CA': 1200,
    'WA': 700,
    'OR': 700,
    'NV': 400,
    'AZ': 400,
    'CO': 350,
}

# Region mapping by state (3 regions total)
# Region 1: Midwest, Region 2: East, Region 3: West
STATE_TO_REGION = {
    # Midwest = Region 1
    'KS': 1, 'MO': 1, 'OK': 1, 'TX': 1, 'NE': 1, 'IA': 1, 'IL': 1,
    'IN': 1, 'OH': 1, 'MI': 1, 'WI': 1, 'MN': 1, 'ND': 1, 'SD': 1,
    # East = Region 2
    'NC': 2, 'GA': 2, 'FL': 2, 'PA': 2, 'VA': 2, 'SC': 2, 'NY': 2,
    # West = Region 3
    'CA': 3, 'WA': 3, 'OR': 3, 'NV': 3, 'AZ': 3, 'CO': 3,
}

# Business segment distribution for sellers (~50% Core, ~15% Enterprise, ~35% Expansion)
BUSINESS_SEGMENT_DISTRIBUTION = {
    'Core': 0.50,
    'Enterprise': 0.15,
    'Expansion': 0.35,
}

# Cities by state
STATE_CITIES = {
    'KS': ['Wichita', 'Kansas City', 'Topeka', 'Overland Park'],
    'MO': ['Kansas City', 'St Louis', 'Springfield', 'Columbia'],
    'OK': ['Oklahoma City', 'Tulsa', 'Norman', 'Broken Arrow'],
    'TX': ['Dallas', 'Houston', 'Austin', 'San Antonio'],
    'NE': ['Omaha', 'Lincoln', 'Bellevue'],
    'IA': ['Des Moines', 'Cedar Rapids', 'Davenport'],
    'IL': ['Chicago', 'Springfield', 'Peoria'],
    'IN': ['Indianapolis', 'Fort Wayne', 'Evansville'],
    'OH': ['Columbus', 'Cleveland', 'Cincinnati'],
    'MI': ['Detroit', 'Grand Rapids', 'Lansing'],
    'WI': ['Milwaukee', 'Madison', 'Green Bay'],
    'MN': ['Minneapolis', 'St Paul', 'Rochester'],
    'ND': ['Fargo', 'Bismarck'],
    'SD': ['Sioux Falls', 'Rapid City'],
    'NC': ['Charlotte', 'Raleigh', 'Greensboro'],
    'GA': ['Atlanta', 'Savannah', 'Augusta'],
    'FL': ['Jacksonville', 'Miami', 'Tampa', 'Orlando'],
    'PA': ['Pittsburgh', 'Philadelphia', 'Harrisburg'],
    'VA': ['Virginia Beach', 'Richmond', 'Norfolk'],
    'SC': ['Charleston', 'Columbia', 'Greenville'],
    'NY': ['New York', 'Buffalo', 'Rochester'],
    'CA': ['Los Angeles', 'San Francisco', 'San Diego', 'Sacramento'],
    'WA': ['Seattle', 'Spokane', 'Tacoma'],
    'OR': ['Portland', 'Eugene', 'Salem'],
    'NV': ['Las Vegas', 'Reno'],
    'AZ': ['Phoenix', 'Tucson', 'Mesa'],
    'CO': ['Denver', 'Colorado Springs', 'Aurora'],
}

# Subcategories by category
SUBCATEGORIES = {
    'Construction': ['Excavators', 'Dozers', 'Wheel Loaders', 'Skid Steers'],
    'Ag Equipment': ['Tractors', 'Combines', 'Planters', 'Harvesters'],
    'Truck/Trailer': ['Pickup Trucks', 'Semi Tractors', 'Dump Trucks', 'Box Trucks'],
    'Passenger': ['Sedans', 'SUVs', 'Minivans', 'Coupes']
}

# Makes/models by category
MAKES_MODELS = {
    'Construction': [
        ('Caterpillar', ['330', '349', '950M', '962M', '972M', 'D6', 'D8', 'D9']),
        ('Komatsu', ['PC210', 'PC290', 'WA470', 'WA500', 'D65', 'D85', 'D155']),
        ('John Deere', ['210G', '350G', '644K', '724K', '850K', '950K']),
        ('Volvo', ['EC220', 'EC300', 'EC480', 'L120H', 'L150H', 'L220H']),
    ],
    'Ag Equipment': [
        ('John Deere', ['6155R', '7230R', '8320R', '8370R', 'S780', 'S790', 'X9 1100', 'DB60', '1775NT']),
        ('Case IH', ['Magnum 280', 'Magnum 340', 'Magnum 380', '8250', '9250', '1255', '2150']),
        ('New Holland', ['T7.270', 'T8.380', 'T9.565', 'CR8.90', 'CR10.90']),
        ('Massey Ferguson', ['8735', '8737']),
    ],
    'Truck/Trailer': [
        ('Ford', ['F-250', 'F-350', 'F-450']),
        ('Chevrolet', ['Silverado 2500', 'Silverado 3500']),
        ('Ram', ['2500', '3500']),
        ('Peterbilt', ['348', '389', '567', '579']),
        ('Kenworth', ['T680', 'T880', 'W900']),
        ('Freightliner', ['Cascadia', 'Columbia', 'Century']),
    ],
    'Passenger': [
        ('Toyota', ['Camry', 'Corolla', 'RAV4', 'Highlander']),
        ('Honda', ['Accord', 'Civic', 'CR-V', 'Pilot']),
        ('Ford', ['Fusion', 'Escape', 'Explorer']),
        ('Chevrolet', ['Malibu', 'Equinox', 'Traverse']),
    ]
}

# Fee structure
FEE_TYPES = {
    'Seller Service Fee': 200,
    'Lot Fee': 150,
    'Power Washing': 225,      # 60% of items
    'Decal Removal': 100,      # 20% of items
}

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def generate_auction_dates():
    """Generate all auction dates (Tue/Wed/Thu) from Aug-Dec 2025, excluding holidays."""
    dates = []
    start_date = datetime(2025, 8, 1)
    end_date = datetime(2025, 12, 31)
    
    # Holidays to skip
    holidays = [
        datetime(2025, 11, 27),  # Thanksgiving
        datetime(2025, 12, 25),  # Christmas
    ]
    
    current = start_date
    while current <= end_date:
        # Tuesday=1, Wednesday=2, Thursday=3 (weekday() returns 0=Monday)
        if current.weekday() in [1, 2, 3]:  # Tue, Wed, Thu
            if current not in holidays:
                dates.append(current)
        current += timedelta(days=1)
    
    return dates

def get_week_number(date):
    """Get week number from start of August 2025."""
    start = datetime(2025, 8, 1)
    delta = date - start
    return (delta.days // 7) + 1

def items_per_day(date):
    """Determine how many items to sell on this date."""
    week = get_week_number(date)
    month = date.month
    
    # Week 15 slowdown (Thanksgiving week, late November)
    if week == 15:
        return random.randint(180, 220)  # ~200 items/day (lower)
    
    # December is the big month (833/day avg)
    if month == 12:
        return random.randint(750, 900)
    
    # Normal Aug-Nov (~313/day avg)
    return random.randint(280, 350)

def get_category_distribution(date):
    """Get category distribution for this date (special handling for week 10 dip)."""
    week = get_week_number(date)
    
    # Week 10 is the dip week (late October)
    if week == 10:
        return WEEK_10_DISTRIBUTION
    
    return CATEGORY_DISTRIBUTION

def generate_icn():
    """Generate a unique ICN (Item Control Number) like YU6014."""
    letters = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=2))
    numbers = ''.join(random.choices('0123456789', k=4))
    return letters + numbers

def generate_customer_name():
    """Generate realistic customer name."""
    first_names = ['John', 'Michael', 'David', 'James', 'Robert', 'William', 'Richard', 'Thomas',
                   'Mary', 'Patricia', 'Jennifer', 'Linda', 'Elizabeth', 'Susan', 'Jessica', 'Sarah',
                   'Mark', 'Donald', 'Steven', 'Paul', 'Andrew', 'Joshua', 'Kevin', 'Brian',
                   'Karen', 'Nancy', 'Betty', 'Helen', 'Sandra', 'Donna', 'Carol', 'Ruth']
    
    last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis',
                  'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson',
                  'Thomas', 'Taylor', 'Moore', 'Jackson', 'Martin', 'Lee', 'Thompson', 'White',
                  'Harris', 'Clark', 'Lewis', 'Robinson', 'Walker', 'Young', 'Allen', 'King']
    
    return random.choice(first_names), random.choice(last_names)

def pick_weighted_state():
    """Pick a state based on distribution weights."""
    states = list(STATE_DISTRIBUTION.keys())
    weights = list(STATE_DISTRIBUTION.values())
    return random.choices(states, weights=weights, k=1)[0]

def get_region_district_territory(state):
    """
    Calculate region, district, and territory for a given state.
    
    3 Regions: Midwest (1), East (2), West (3)
    12 Districts: 4 per region, numbered globally 1-12
    96 Territories: 8 per district, numbered globally 1-96
    """
    region_id = STATE_TO_REGION[state]
    
    # Districts 1-12 globally: Region 1 → 1-4, Region 2 → 5-8, Region 3 → 9-12
    district_base = (region_id - 1) * 4
    district_id = random.randint(district_base + 1, district_base + 4)
    
    # Territories 1-96 globally: District 1 → 1-8, District 2 → 9-16, etc.
    territory_base = (district_id - 1) * 8
    territory_id = random.randint(territory_base + 1, territory_base + 8)
    
    return region_id, district_id, territory_id

def pick_seller_for_category(sellers, category):
    """
    Pick a seller with preference based on category.
    
    Construction → prefer Enterprise sellers
    Passenger → prefer Core sellers
    Others → prefer Core/Expansion sellers
    """
    if category == 'Construction':
        # 70% chance to pick Enterprise if available, otherwise random
        enterprise_sellers = [s for s in sellers if s.get('business_segment') == 'Enterprise']
        if enterprise_sellers and random.random() < 0.70:
            return random.choice(enterprise_sellers)
    
    elif category == 'Passenger':
        # 60% chance to pick Core if available, otherwise random
        core_sellers = [s for s in sellers if s.get('business_segment') == 'Core']
        if core_sellers and random.random() < 0.60:
            return random.choice(core_sellers)
    
    # Default: pick random seller
    return random.choice(sellers)

# ============================================================================
# MAIN GENERATION FUNCTIONS
# ============================================================================

def generate_customers():
    """Generate customer records (buyers and sellers)."""
    customers = []
    customer_id = 1
    
    # Generate 2000 buyers (no business_segment needed)
    print("Generating 2000 buyers...")
    for i in range(2000):
        first, last = generate_customer_name()
        state = pick_weighted_state()
        customers.append({
            'customer_id': customer_id,
            'first_name': first,
            'last_name': last,
            'email': f"{first.lower()}.{last.lower()}{random.randint(1,999)}@email.com",
            'state': state,
            'customer_type': 'buyer',
            'business_segment': None,
            'active': 1
        })
        customer_id += 1
    
    # Generate 500 sellers with business_segment
    print("Generating 500 sellers...")
    for i in range(500):
        first, last = generate_customer_name()
        state = pick_weighted_state()
        
        # Assign business segment based on distribution
        business_segment = random.choices(
            list(BUSINESS_SEGMENT_DISTRIBUTION.keys()),
            weights=list(BUSINESS_SEGMENT_DISTRIBUTION.values()),
            k=1
        )[0]
        
        customers.append({
            'customer_id': customer_id,
            'first_name': first,
            'last_name': last,
            'email': f"{first.lower()}.{last.lower()}{random.randint(1,999)}@email.com",
            'state': state,
            'customer_type': 'seller',
            'business_segment': business_segment,
            'active': 1
        })
        customer_id += 1
    
    # Generate 150 who are both (also need business_segment)
    print("Generating 150 customers who are both buyers and sellers...")
    for i in range(150):
        first, last = generate_customer_name()
        state = pick_weighted_state()
        
        # Assign business segment based on distribution
        business_segment = random.choices(
            list(BUSINESS_SEGMENT_DISTRIBUTION.keys()),
            weights=list(BUSINESS_SEGMENT_DISTRIBUTION.values()),
            k=1
        )[0]
        
        customers.append({
            'customer_id': customer_id,
            'first_name': first,
            'last_name': last,
            'email': f"{first.lower()}.{last.lower()}{random.randint(1,999)}@email.com",
            'state': state,
            'customer_type': 'both',
            'business_segment': business_segment,
            'active': 1
        })
        customer_id += 1
    
    return customers

def generate_items_bids_fees(customers):
    """Generate items, bids, and fees together to maintain relationships."""
    items = []
    bids = []
    fees = []
    
    item_id = 1
    bid_id = 1
    fee_id = 1
    
    # Get seller and buyer pools
    sellers = [c for c in customers if c['customer_type'] in ['seller', 'both']]
    buyers = [c for c in customers if c['customer_type'] in ['buyer', 'both']]
    
    # Generate auction dates
    auction_dates = generate_auction_dates()
    print(f"Generated {len(auction_dates)} auction days from Aug-Dec 2025")
    
    # Track items per state to match distribution
    items_by_state = defaultdict(int)
    target_by_state = STATE_DISTRIBUTION.copy()
    
    # Generate items for each auction day
    for auction_date in auction_dates:
        num_items = items_per_day(auction_date)
        category_dist = get_category_distribution(auction_date)
        
        date_str = auction_date.strftime('%Y-%m-%d')
        week = get_week_number(auction_date)
        
        print(f"  {date_str} (Week {week}): Generating {num_items} items...")
        
        for _ in range(num_items):
            # Pick category based on distribution
            category = random.choices(
                list(category_dist.keys()),
                weights=list(category_dist.values()),
                k=1
            )[0]
            
            # Pick state (weighted by remaining quota)
            remaining_states = {s: max(0, target - items_by_state[s]) 
                               for s, target in target_by_state.items()}
            if sum(remaining_states.values()) == 0:
                # All quotas met, pick randomly
                state = pick_weighted_state()
            else:
                states = list(remaining_states.keys())
                weights = list(remaining_states.values())
                state = random.choices(states, weights=weights, k=1)[0]
            
            items_by_state[state] += 1
            city = random.choice(STATE_CITIES[state])
            
            # Pick subcategory, make, model
            subcategory = random.choice(SUBCATEGORIES[category])
            make, models = random.choice(MAKES_MODELS[category])
            model = random.choice(models)
            
            # Generate prices
            min_price, max_price = PRICE_RANGES[category]
            starting_bid = random.randint(int(min_price * 0.5), int(min_price * 0.8))
            reserve_price = random.randint(int(min_price * 0.7), int(min_price * 0.9))
            hammer = random.randint(min_price, max_price)
            reserve_met = 1 if hammer >= reserve_price else 0
            buyers_premium = int(hammer * 0.10)  # 10% buyer premium
            contract_price = hammer + buyers_premium
            
            # Get region, district, territory from state
            region_id, district_id, territory_id = get_region_district_territory(state)
            
            # Pick seller (with category preference) and buyer
            seller = pick_seller_for_category(sellers, category)
            buyer = random.choice(buyers)
            
            # Create item record
            item = {
                'unique_id': item_id,
                'icn': generate_icn(),
                'auctiondate': date_str,
                'year': random.randint(1990, 2024),
                'make': make,
                'model': model,
                'category': category,
                'subcategory': subcategory,
                'location_state': state,
                'location_city': city,
                'starting_bid': starting_bid,
                'reserve_price': reserve_price,
                'hammer': hammer,
                'buyers_premium': buyers_premium,
                'contract_price': contract_price,
                'reserve_met': reserve_met,
                'seller_id': seller['customer_id'],
                'buyer_id': buyer['customer_id'],
                'num_bids': random.randint(1, 15),
                'region_id': region_id,
                'district_id': district_id,
                'territory_id': territory_id,
                'business_segment': seller.get('business_segment', 'Core')
            }
            items.append(item)
            
            # Generate bids for this item
            num_bids = item['num_bids']
            current_bid = starting_bid
            
            for bid_num in range(num_bids):
                # Pick a random bidder
                bidder = random.choice(buyers)
                
                # Increment bid amount
                increment = random.randint(100, 1000)
                current_bid += increment
                
                # Last bid should be the hammer price and from the winner
                is_winning = 1 if bid_num == num_bids - 1 else 0
                if is_winning:
                    current_bid = hammer
                    bidder_id = buyer['customer_id']
                else:
                    bidder_id = bidder['customer_id']
                
                # Bid timestamp (during auction day)
                bid_time = auction_date + timedelta(hours=random.randint(8, 17), 
                                                    minutes=random.randint(0, 59))
                
                bids.append({
                    'bid_id': bid_id,
                    'item_id': item_id,
                    'bidder_id': bidder_id,
                    'bid_amount': current_bid,
                    'bid_timestamp': bid_time.strftime('%Y-%m-%d %H:%M:%S'),
                    'is_winning_bid': is_winning
                })
                bid_id += 1
            
            # Generate fees for this item
            # Always have Seller Service Fee and Lot Fee
            fees.append({
                'fee_id': fee_id,
                'item_id': item_id,
                'fee_type': 'Seller Service Fee',
                'fee_amount': FEE_TYPES['Seller Service Fee']
            })
            fee_id += 1
            
            fees.append({
                'fee_id': fee_id,
                'item_id': item_id,
                'fee_type': 'Lot Fee',
                'fee_amount': FEE_TYPES['Lot Fee']
            })
            fee_id += 1
            
            # 60% chance of Power Washing
            if random.random() < 0.60:
                fees.append({
                    'fee_id': fee_id,
                    'item_id': item_id,
                    'fee_type': 'Power Washing',
                    'fee_amount': FEE_TYPES['Power Washing']
                })
                fee_id += 1
            
            # 20% chance of Decal Removal
            if random.random() < 0.20:
                fees.append({
                    'fee_id': fee_id,
                    'item_id': item_id,
                    'fee_type': 'Decal Removal',
                    'fee_amount': FEE_TYPES['Decal Removal']
                })
                fee_id += 1
            
            item_id += 1
    
    print(f"\nGenerated {len(items)} items across {len(auction_dates)} auction days")
    print(f"Generated {len(bids)} total bids")
    print(f"Generated {len(fees)} fee records")
    
    # Print distribution summary
    print("\n=== Items by State ===")
    for state in sorted(items_by_state.keys(), key=lambda s: items_by_state[s], reverse=True):
        print(f"  {state}: {items_by_state[state]:>5} items (target: {target_by_state[state]})")
    
    return items, bids, fees

def write_csv(filename, data, fieldnames):
    """Write data to CSV file."""
    filepath = f'seeds/{filename}'
    print(f"\nWriting {filepath}...")
    
    with open(filepath, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    
    print(f"  Wrote {len(data)} records")

# ============================================================================
# MAIN
# ============================================================================

def main():
    print("=" * 80)
    print("Purple Wave Auction Data Generator")
    print("=" * 80)
    print("\nGenerating Bronze layer data...")
    print(f"Target: 25,000 items across Aug-Dec 2025")
    print(f"Customers: ~2,650 total (2,000 buyers + 500 sellers + 150 both)")
    print()
    
    # Generate customers first
    customers = generate_customers()
    
    # Generate items, bids, and fees
    items, bids, fees = generate_items_bids_fees(customers)
    
    # Write CSV files
    write_csv('customers.csv', customers, 
              ['customer_id', 'first_name', 'last_name', 'email', 'state', 'customer_type', 'business_segment', 'active'])
    
    write_csv('items_v2.csv', items,
              ['unique_id', 'icn', 'auctiondate', 'year', 'make', 'model', 'category', 'subcategory',
               'location_state', 'location_city', 'starting_bid', 'reserve_price', 'hammer', 
               'buyers_premium', 'contract_price', 'reserve_met', 'seller_id', 'buyer_id', 'num_bids',
               'region_id', 'district_id', 'territory_id', 'business_segment'])
    
    write_csv('bids.csv', bids,
              ['bid_id', 'item_id', 'bidder_id', 'bid_amount', 'bid_timestamp', 'is_winning_bid'])
    
    write_csv('fees.csv', fees,
              ['fee_id', 'item_id', 'fee_type', 'fee_amount'])
    
    print("\n" + "=" * 80)
    print("GENERATION COMPLETE!")
    print("=" * 80)
    print("\nNext steps:")
    print("1. Run: dbt seed")
    print("2. Run: dbt run")
    print("3. Check the data in your database")

if __name__ == '__main__':
    main()