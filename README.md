# gajana
An automated personal finance tracker that fetches transactions from multiple sources and accumulates the data and builds dashboards

## Contribution guidelines
* Create a new branch with naming `new-feature-request` from `master`
* Make the code changes
* Add sufficient comments to ensure code is readable
* Install lefthook for githooks to be active
* Run format / lint to ensure code is clean `yarn lint` and `yarn format`
* Push the branch and submit a PR

## Setup
### Dependencies
* [NVM](https://github.com/nvm-sh/nvm)
* Node v16
  * `nvm install 16.16.0`
  * `nvm use 16.16.0`
* Global node dependencies
  * `nvm install -g yarn nx lefthook`
* Project dependencies
  * `yarn install`
* Githooks
  * `lefthook install`

### Run local
`yarn local`
The server should be running at `http://localhost:3333` and the API server at `http://localhost:3333/api`

### Build
`yarn build`
