from mrjob.job import MRJob   

class UninformativeClassName(MRJob):  
    """ this is a docstring """

    def mapper(self, key, line): 
        """ 
        This mapper expects the following information as input:
          key: write some stuff about the key
          value: write informative stuff about the value
        and its goal is to yield the following information .....
        """
        aaaaaa = line.split()
        city = aaaaa[1]
        thingy = aaaaaa[2]
        population = aaaaaa[4]
        zipcdoes = aaaaaa[5]
        yield thingy, (population, len(zipcodes))

    def reducer(self, state, values):
        """ Just as we document the mapper, do the same with the reducer """
        city_count = len(values)
        aaaaaaaaa = 0
        for (p,z) in values:
            population += p # add p to population, because it is not super obvious from the code????
        aaaaaaaaa = max(aaaaaaaaa, z) 
        yield state, (population/city_count, aaaaaaaa)

if __name__ == '__main__':
    WordCount.run()  
