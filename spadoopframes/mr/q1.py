#!/usr/bin/env python3
from mrjob.job import MRJob
from mrjob.step import MRStep

class CityStats(MRJob):
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   combiner=self.combiner,
                   reducer=self.reducer)
        ]
    
    def mapper(self, _, line):
        parts = line.strip().split('\t')
        
        # Filter bad lines - need at least 6 fields
        if len(parts) < 6:
            return
            
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
                return
            
            # Calculate people per zip
            people_per_zip = population / num_zips
            
            # Determine if dense or super dense
            is_dense = 1 if population >= 500 * num_zips else 0
            is_super_dense = 1 if population >= 1000 * num_zips else 0
            
            # Emit: state -> (people_per_zip, 1, is_dense, is_super_dense)
            yield state, (people_per_zip, 1, is_dense, is_super_dense)
        except (ValueError, IndexError):
            return
    
    def combiner(self, state, values):
        total_people_per_zip = 0.0
        total_cities = 0
        dense_count = 0
        super_dense_count = 0
        
        for ppz, count, dense, super_dense in values:
            total_people_per_zip += ppz
            total_cities += count
            dense_count += dense
            super_dense_count += super_dense
        
        yield state, (total_people_per_zip, total_cities, dense_count, super_dense_count)
    
    def reducer(self, state, values):
        total_people_per_zip = 0.0
        total_cities = 0
        dense_count = 0
        super_dense_count = 0
        
        for ppz, count, dense, super_dense in values:
            total_people_per_zip += ppz
            total_cities += count
            dense_count += dense
            super_dense_count += super_dense
        
        if total_cities > 0:
            avg_ppz = round(total_people_per_zip / total_cities)
            yield state, [avg_ppz, dense_count, super_dense_count]

if __name__ == '__main__':
    CityStats.run()
