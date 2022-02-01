# Serverless Paper Impl

My masters paper impl

## Tasks
### General

- [x] Implement Weilbull Stretched function
- [x] Implement Normalizer function 
- [x] Implement "restore time" calculator based on function deps
- [x] Implement Russuan Roullete
- [x] Implement Similarity Function
- [x] Create function data generator
  - [x] Fully-random generator
    - [x] Uniform Dist
    - [x] Normal Dist
  - [x] Package-Frequency aware generator
    - [x] Uniform Dist
    - [x] Normal Dist
- [x] Implement the Synthetic Data Generator
  - [x] Random Invocation time generator
  - [x] Invocation Frequency aware generator
- [x] Write the event simulator based on synthetic generator
- [ ] Implement schedulers
  - [x] No-merge
  - [x] Random Wait
  - [x] Static Wait
  - [x] Dynamic Wait
    - [x] Implement Co-op Network
    - [x] Implement Digestra
    - [x] Policies
      - [x] Always Neutral
      - [x] Context Based
- [ ] Evaluate scheduler metrics
  - [ ] Using Fully Random Synthetic Data
    - [ ] No-merge
    - [ ] Random Wait
    - [ ] Static Wait
    - [ ] Dynamic Wait
  - [ ] Using Realistic Synthetic Data
    - [ ] No-merge
    - [ ] Random Wait
    - [ ] Static Wait
    - [ ] Dynamic Wait


- Massure approaches by
  - Response time = Restoration Duration
  - Cost
  - Throughput
  - Response/Cost Ratio

#### No-Merge approach

Terminate container after function terminates

### Random-Wait Approach

Wait for a random time after function terminates

### Static-Wait Approach

Wait for 5 mins after function terminates

### Dynamic-Wait Approach (this paper)

- [x] Create coop network
  - [x] Weight Evaporation
  - [x] Co-op probability calculator

- **WarmContainer**: Number of previous function runs to consider = 1
- **Container**::Number of reusing the container = k = 5
- **Contaner**::rMax = 20 min _// can't wait more than 20 min_

#### Policy

Waiting container should consider these data:

1. Similarity of its function to functions inside the running containers
2. Previous merge fail or success status for its function
3. co-op score 
4. budget 


## Statistical Distributions

- Normalaizer: **Stretched Weilbul** with `a = 5`, `b = 2`
- Number of Function dependencies: **Normal**, u = 50, stddev = 15
- Function Duration: **Chi-Square**, `v = 2.1`
- Function Run Request Every: **Normal**, `u = 30`, `stddev = 10`
- Random Wait Scheduler Wait Time: **Normal**, `u = 240`, `stddev = 50`
- Container Creation Time Cose: **Chi-squared**, `v = 5` 