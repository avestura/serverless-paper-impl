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
- [ ] Write the event simulator based on synthetic generator
- [ ] Implement schedulers
  - [ ] No-merge
  - [ ] Random Wait
  - [ ] Static Wait
  - [ ] Dynamic Wait
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
  - Response time
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

- [ ] Create coop network
  - [ ] Weight Evaporation
  - [ ] Co-op probability calculator


## Statistical Distributions

- Normalaizer: Weilbul
- Number of Function dependencies: Normal, u = 50, stddev = 15
- Function Duration: Chi-Square, v = 2.1
- Function Run Request Every: Normal, u = 30, stddev = 10
- Random Wait Scheduler Wait Time: Normal, u = 240, stddev = 50