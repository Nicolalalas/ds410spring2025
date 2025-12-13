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
        parts = line.strip().split('\t')
        
        if len(parts) < 6:
            return
            
        try:
            state = parts[1]
            population = int(parts[3])
            zip_str = parts[4].strip()
            
            # Critical: Handle empty string correctly
            # "".split() gives [] but "".split("\\s+") gives [""]
            # We check isEmpty first
            if not zip_str:  # empty string
                num_zips = 0
            else:
                num_zips = len(zip_str.split())
            
            if num_zips == 0:
                return
            
            people_per_zip = population / num_zips
            is_dense = 1 if population >= 500 * num_zips else 0
            is_super_dense = 1 if population >= 1000 * num_zips else 0
            
            # Yield (state, (sum_ppz, count, dense_count, super_dense_count))
            yield state, (people_per_zip, 1, is_dense, is_super_dense)
            
        except (ValueError, IndexError):
            return
    
    def combiner(self, state, values):
        # Aggregate locally before sending to reducer
        sum_ppz = 0.0
        count = 0
        dense = 0
        super_dense = 0
        
        for ppz, cnt, d, sd in values:
            sum_ppz += ppz
            count += cnt
            dense += d
            super_dense += sd
        
        # Emit aggregated values
        yield state, (sum_ppz, count, dense, super_dense)
    
    def reducer(self, state, values):
        sum_ppz = 0.0
        count = 0
        dense = 0
        super_dense = 0
        
        for ppz, cnt, d, sd in values:
            sum_ppz += ppz
            count += cnt
            dense += d
            super_dense += sd
        
        if count > 0:
            avg_ppz = round(sum_ppz / count)
            yield state, [avg_ppz, dense, super_dense]

if __name__ == '__main__':
    CityStats.run()
