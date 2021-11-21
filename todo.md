# Todo


## General

- [ ] Write "restore time" calculator based on function deps
- [ ] Write Similarity Function
- [ ] Create function data generator
  - [ ] Fully-random generator
  - [ ] Package-Frequency aware generator
- [ ] Write the Synthetic Data Generator
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

- [ ] Create function data generator using `packageData` folder
- [ ] Create coop network
  - [ ] Weight Evaporation
  - [ ] Co-op probability calculator