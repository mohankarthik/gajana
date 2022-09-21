# gajana

An automated personal finance tracker that fetches transactions from multiple sources and accumulates the data and builds dashboards

## Contribution guidelines

- Create a new branch with naming `new-feature-request` from `master`
- Make the code changes
- Add sufficient comments to ensure code is readable
- Install lefthook for githooks to be active
- Run format / lint to ensure code is clean `yarn lint` and `yarn format`
- Push the branch and submit a PR

## Setup

### Dependencies

- [NVM](https://github.com/nvm-sh/nvm)
- Node v16
  - `nvm install 16.16.0`
  - `nvm use 16.16.0`
- Global node dependencies
  - `nvm install -g yarn nx lefthook`
- Project dependencies
  - `yarn install`
- Githooks
  - `lefthook install`
- Environment configuration
  - Copy `apps/api/.env.local.example` and rename to `apps/api/.env.local`
  - Fill in the values from your Mongo repository and your Google credentials. See steps below

#### Mongo

- Create a new mongo DB (either using the [free MongoDb Atlas cluster](https://www.mongodb.com/pricing) or a self hosted version
- Ensure that your local IP address has access to the DB. If using Atlas, either whitelist your IP, or enable all IP `0.0.0.0/0`
- Copy the username, password and hostname and enter them in the `apps/api/.env.local` file

#### Google

- Follow the steps [here](https://developers.google.com/workspace/guides/create-credentials) to setup a Google credentials
- Copy the `client_id` and `client_secret` and enter them in `apps/api/.env.local` file
- Run `node ./scripts/generate-google-tokens.js` and follow the steps on the console to oauth your gmail account and generate a `token` file saved at `./scripts/token.json`
- Go to Mongo, and under the `googletokens` collection, add the contents of the token file so it can be used.

### Run local

`yarn local`
The server should be running at `http://localhost:3333` and the API server at `http://localhost:3333/api`

### Build and run

`yarn build`
`yarn start`
