# Todo


## General

- [ ] Implement "restore time" calculator based on function deps
- [x] Implement Russuan Roullete
- [ ] Implement Similarity Function
- [x] Create function data generator
  - [x] Fully-random generator
    - [x] Uniform Dist
    - [x] Normal Dist
  - [x] Package-Frequency aware generator
    - [x] Uniform Dist
    - [x] Normal Dist
- [ ] Implement the Synthetic Data Generator
  - [ ] Random Invocation time generator
  - [ ] Invocation Frequency aware generator
- [ ] Massure approaches by
  - [ ] Response time
  - [ ] Cost
  - [ ] Throughput
  - [ ] Response/Cost Ratio

### No-Merge approach

Terminate container after function terminates

## Random-Wait Approach

Wait for a random time after function terminates

## Static-Wait Approach

Wait for 5 mins after function terminates

## Dynamic-Wait Approach (this paper)

- [ ] Create coop network
  - [ ] Weight Evaporation
  - [ ] Co-op probability calculator