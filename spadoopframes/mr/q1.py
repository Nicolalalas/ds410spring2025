#!/usr/bin/env python3
import sys

def mapper():
    for line in sys.stdin:
        line = line.strip()
        parts = line.split('\t')
        
        # Filter bad lines - need at least 6 fields
        if len(parts) < 6:
            continue
            
        try:
            state = parts[1]
            population = int(parts[3])
            zip_codes_str = parts[4].strip()
            
            # Count zip codes
            if zip_codes_str == '':
                num_zips = 0
            else:
                num_zips = len(zip_codes_str.split())
            
            # Skip cities with 0 zip codes
            if num_zips == 0:
                continue
            
            # Calculate people per zip
            people_per_zip = population / num_zips
            
            # Determine if dense or super dense
            is_dense = 1 if population >= 500 * num_zips else 0
            is_super_dense = 1 if population >= 1000 * num_zips else 0
            
            # Emit: state, (people_per_zip, num_zips, is_dense, is_super_dense, 1)
            print(f"{state}\t{people_per_zip},{num_zips},{is_dense},{is_super_dense},1")
        except (ValueError, IndexError):
            continue

def combiner():
    current_state = None
    total_people_per_zip = 0.0
    total_cities = 0
    dense_count = 0
    super_dense_count = 0
    
    for line in sys.stdin:
        line = line.strip()
        parts = line.split('\t')
        
        if len(parts) != 2:
            continue
            
        state = parts[0]
        values = parts[1].split(',')
        
        if len(values) != 5:
            continue
            
        try:
            ppz = float(values[0])
            nz = int(values[1])
            dense = int(values[2])
            super_dense = int(values[3])
            count = int(values[4])
            
            if current_state == state:
                total_people_per_zip += ppz
                total_cities += count
                dense_count += dense
                super_dense_count += super_dense
            else:
                if current_state is not None:
                    print(f"{current_state}\t{total_people_per_zip},{total_cities},{dense_count},{super_dense_count}")
                
                current_state = state
                total_people_per_zip = ppz
                total_cities = count
                dense_count = dense
                super_dense_count = super_dense
        except ValueError:
            continue
    
    if current_state is not None:
        print(f"{current_state}\t{total_people_per_zip},{total_cities},{dense_count},{super_dense_count}")

def reducer():
    current_state = None
    total_people_per_zip = 0.0
    total_cities = 0
    dense_count = 0
    super_dense_count = 0
    
    for line in sys.stdin:
        line = line.strip()
        parts = line.split('\t')
        
        if len(parts) != 2:
            continue
            
        state = parts[0]
        values = parts[1].split(',')
        
        if len(values) != 4:
            continue
            
        try:
            ppz = float(values[0])
            count = int(values[1])
            dense = int(values[2])
            super_dense = int(values[3])
            
            if current_state == state:
                total_people_per_zip += ppz
                total_cities += count
                dense_count += dense
                super_dense_count += super_dense
            else:
                if current_state is not None:
                    avg_ppz = round(total_people_per_zip / total_cities)
                    print(f"{current_state}\t[{avg_ppz}, {dense_count}, {super_dense_count}]")
                
                current_state = state
                total_people_per_zip = ppz
                total_cities = count
                dense_count = dense
                super_dense_count = super_dense
        except (ValueError, ZeroDivisionError):
            continue
    
    if current_state is not None and total_cities > 0:
        avg_ppz = round(total_people_per_zip / total_cities)
        print(f"{current_state}\t[{avg_ppz}, {dense_count}, {super_dense_count}]")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        if sys.argv[1] == "map":
            mapper()
        elif sys.argv[1] == "combine":
            combiner()
        elif sys.argv[1] == "reduce":
            reducer()
