

## Calculating and reporting your findings
You'll be submitting a report along with your code that provides commentary on the tasks below.  

1. **(3 points)** Implement the `exact_F2` function. The function accepts an RDD of strings as an input. The output should be exactly `F2 = sum(Fs^2)`, where `Fs` is the number of occurrences of plate `s` and the sum is taken over all plates. This can be achieved in one line using the `map` and `reduceByKey` methods of the RDD class. Run `exact_F2` locally **and** on GCP with 1 driver and 4 machines having 2 x N1 cores. Copy the results to your report. Terminate the program if it runs for longer than 30 minutes.


def exact_F2(x: RDD[String]) : Long = {
  val pairRdd = x.map(a=>(a,1))
  val f2 = pairRdd.reduceByKey(_ + _)
  val result=f2.map(i=>scala.math.pow(i._2,2)).reduce(_+_)
  return result.toLong
}

------------------------------------------------------------------
Output:
Local:
Exact F2. Time elapsed:60s. Estimate: 8567966130
GCP:
Exact F2. Time elapsed:46s. Estimate: 8567966130
------------------------------------------------------------------

2. **(3 points)** Implement the `Tug_of_War` function. The function accepts an RDD of strings, a parameter `width`, and a parameter `depth` as inputs. It should run `width * depth` Tug-of-War sketches, group the outcomes into groups of size `width`, compute the means of each group, and then return the median of the `depth` means in approximating F2. A 4-universal hash function class `four_universal_Radamacher_hash_function`, which generates a hash function from a 4-universal family, has been provided for you. The generated function `hash(s: String)` will hash a string to 1 or -1, each with a probability of 50%. Once you've implemented the function, set `width` to 10 and `depth` to 3. Run `Tug_of_War` locally **and** on GCP with 1 driver and 4 machines having 2 x N1 cores. Copy the results to your report. Terminate the program if it runs for longer than 30 minutes. **Please note** that the algorithm won't be significantly faster than `exact_F2` since the number of different cars is not large enough for the memory to become a bottleneck. Additionally, computing `width * depth` hash values of the license plate strings requires considerable overhead. That being said, executing with `width = 1` and `depth = 1` should generally still be faster.

code:
def Tug_of_War(x: RDD[String], width: Int, depth:Int) : Long = {
  val h = Seq.fill(depth)(Seq.fill(width)(new four_universal_Radamacher_hash_function()))

  def p0 = (accu1: Seq[Seq[Long]], accu2: Seq[Seq[Long]]) => Seq.range(0, depth).map(i => Seq.range(0,width).map(j => accu1(i)(j) + accu2(i)(j)))
  def p1 = (accu1: Seq[Seq[Long]], s: String) => Seq.range(0, depth).map(i => Seq.range(0,width).map(j => accu1(i)(j) + h(i)(j).hash(s)))

  var z = x.aggregate(Seq.fill(depth)(Seq.fill(width)(0.asInstanceOf[Long])))( p1, p0).map(depSeq => depSeq.map(x => x * x))
  val result = z.map(depSeq => depSeq.reduce(_+_)/width).sortWith(_ < _)( depth/2)
  return result
}
------------------------------------------------------------------
Output:
Local:
Tug-of-War F2 Approximation. Width :10. Depth: 3. Time elapsed:94s. Estimate: 9852852099
GCP:
Tug-of-War F2 Approximation. Width :10. Depth: 3. Time elapsed:121s. Estimate: 7780988795
------------------------------------------------------------------

3. **(3 points)** Implement the `BJKST` function. The function accepts an RDD of strings, a parameter `width`, and a parameter `trials` as inputs. `width` denotes the maximum bucket size of each sketch. The function should run `trials` sketches and return the median of the estimates of the sketches. A template of the `BJKSTSketch` class is also included in the sample code. You are welcome to finish its methods and apply that class or write your own class from scratch. A 2-universal hash function class `hash_function(numBuckets_in: Long)` has also been provided and will hash a string to an integer in the range `[0, numBuckets_in - 1]`. Once you've implemented the function, determine the smallest `width` required in order to achieve an error of +/- 20% on your estimate. Keeping `width` at that value, set `depth` to 5. Run `BJKST` locally **and** on GCP with 1 driver and 4 machines having 2 x N1 cores. Copy the results to your report. Terminate the program if it runs for longer than 30 minutes.
In order to achieve an error range of +/- 20% on the estimate, we set eqsilon to 0.2 and the memory usage is bounded by 24/(epsilon^2) = 24 / 0.04 = 600.

code:

class BJKSTSketch(bucket_in: Set[(String, Int)] ,  z_in: Int, bucket_size_in: Int) extends Serializable {

  var bucket: Set[(String, Int)] = bucket_in
  var z: Int = z_in

  val BJKST_bucket_size = bucket_size_in;

  def this(s: String, z_of_s: Int, bucket_size_in: Int){
    //A constructor that allows you pass in a single string, zeroes of the string, and the bucket size to initialize the sketch
    this(Set((s, z_of_s )) , z_of_s, bucket_size_in)
  }

  def +(that: BJKSTSketch): BJKSTSketch = {   // Merging two sketches
    var numB = bucket ++ that.bucket
    z = that.z.max(z)
    numB = numB.filter(_._2 >= z)
    while ( numB.size > bucket_size_in){
      z += 1
      numB = numB.filter(_._2 >= z)
    }
    return new BJKSTSketch(numB, z, BJKST_bucket_size)
  }

  def add_string(s: String, z_of_s: Int): BJKSTSketch = {    //add a string to the sketch
    return this + new BJKSTSketch(s, z_of_s, BJKST_bucket_size)
  }
}
def BJKST(x: RDD[String], width: Int, trials: Int) : Double = {

  val h = Seq.fill(trials)(new hash_function(2000000000))

  def p0 = (accu1: Seq[BJKSTSketch], accu2: Seq[BJKSTSketch]) => Seq.range(0, trials).map(i => accu1(i) + accu2(i))
  def p1 = (accu1: Seq[BJKSTSketch], s: String) => Seq.range(0, trials).map( i => accu1(i).add_string(s, h(i).zeroes(h(i).hash(s))) )

  val z = x.aggregate(Seq.range(0, trials).map(i => new BJKSTSketch(x.take(1)(0), h(i).zeroes(h(i).hash(x.take(1)(0))), width)))(p1, p0)
  val result = z.map(b => b.bucket.size * scala.math.pow(2, b.z)).sortWith(_ < _)( trials/2)

  return result

}
------------------------------------------------------------------
Locally
BJKST Algorithm. Bucket Size:600. Trials:5. Time elapsed:62s. Estimate: 8388608.0

GCP:
BJKST Algorithm. Bucket Size:600. Trials:5. Time elapsed:85s. Estimate: 8388608.0
------------------------------------------------------------------
4. **(1 point)** Compare the BJKST algorithm to the exact F0 algorithm and the tug-of-war algorithm to the exact F2 algorithm. Summarize your findings.

exactFO:
  Local:
  Exact F0. Time elapsed:49s. Estimate: 7406649
  GCP:
  Exact F0. Time elapsed:43s. Estimate: 7406649
  ---------------
BJKST:
  Local:
  BJKST Algorithm. Bucket Size:600. Trials:5. Time elapsed:62s. Estimate: 8388608.0
  GCP:
  BJKST Algorithm. Bucket Size:600. Trials:5. Time elapsed:85s. Estimate: 8388608.0
  ---------------
exactF2:
  Local:
  Exact F2. Time elapsed:60s. Estimate: 8567966130
  GCP:
  Exact F2. Time elapsed:46s. Estimate: 8567966130
  ---------------
ToW:
  Local:
  Tug-of-War F2 Approximation. Width :10. Depth: 3. Time elapsed:94s. Estimate: 9852852099
  GCP:
  Tug-of-War F2 Approximation. Width :10. Depth: 3. Time elapsed:121s. Estimate: 7780988795
  ---------------
  Analysis:
    For the BJKST estimation, 8388608/7406649 - 1 = 13.257% which lies within the the 20% error range. Surprisingly the estimate computed locally is the same as the one computed through GCP.

    For the ToW estimation, (local)9852852099 / 8567966130 - 1 = 14.99% and (GCP) 7780988795 / 8567966130 - 1 = -9.18%. The estimations are also fairly precise.

    We noticed that the running time for exactF0 and exactF2 is faster on GCP while that is not the case for BJKST and ToW. We assume that is due to the the sample size being not as large enough. 
