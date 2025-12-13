#!/usr/bin/env python3
from mrjob.job import MRJob
from mrjob.step import MRStep

class CityStats(MRJob):
    
    def steps(self):
        return [
            MRStep(
                mapper=self.mapper,
                combiner=self.combiner,
                reducer=self.reducer
            )
        ]
    
    def mapper(self, _, line):
        # Skip header line
        if line.startswith('city\t'):
            return
            
        parts = line.strip().split('\t')
        
        if len(parts) < 7:  # Need 7 fields now!
            return
            
        try:
            state = parts[1]
            population = int(parts[4])  # population at index 4
            zip_str = parts[5].strip()  # zipcodes at index 5
            
            # Count zip codes - handle empty/null
            if not zip_str or zip_str.strip() == '':
                num_zips = 0
            else:
                num_zips = len(zip_str.split())
            
            # Calculate dense/super dense (even if num_zips is 0)
            is_dense = 1 if num_zips > 0 and population >= 500 * num_zips else 0
            is_super_dense = 1 if num_zips > 0 and population >= 1000 * num_zips else 0
            
            # Emit: (state, (total_population, total_zips, dense_count, super_dense_count))
            yield state, (population, num_zips, is_dense, is_super_dense)
            
        except (ValueError, IndexError):
            return
    
    def combiner(self, state, values):
        total_pop = 0
        total_zips = 0
        dense = 0
        super_dense = 0
        
        for pop, zips, d, sd in values:
            total_pop += pop
            total_zips += zips
            dense += d
            super_dense += sd
        
        yield state, (total_pop, total_zips, dense, super_dense)
    
    def reducer(self, state, values):
        total_pop = 0
        total_zips = 0
        dense = 0
        super_dense = 0
        
        for pop, zips, d, sd in values:
            total_pop += pop
            total_zips += zips
            dense += d
            super_dense += sd
        
        # CRITICAL: sum(population) / sum(num_zips)
        if total_zips > 0:
            avg_ppz = round(total_pop / total_zips)
        else:
            avg_ppz = 0
            
        yield state, [avg_ppz, dense, super_dense]

if __name__ == '__main__':
    CityStats.run()
