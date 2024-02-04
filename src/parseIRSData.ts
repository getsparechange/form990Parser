import Bluebird from "bluebird";
import fs from "fs";
import CharityDataDynamoDB from "somewhere";
import xml2JS from "xml2js";
import bunyan from "bunyan";
import path from "path";
import loopOverAllCharities from "./loopOverAllCharities";

const logger = bunyan.createLogger({ name: "parseIRSData-Main" });

/**
 * Main driver function to parse all our IRS data
 * @param pathToFolder Path to a folder with all the downloaded files from the reference below
 * @see https://www.irs.gov/charities-non-profits/form-990-series-downloads
 */
export default async function parseIRSData(args: { pathToFolder: string }) {
  logger.info(`Starting to parse IRS Data... 🦑`);
  const { pathToFolder } = args;

  await loopOverAllCharities({
    concurrency: 20,
    pathToFolder,

    callback: async (data) => {
      /**
       * Check if we've already created this before
       */
      const existingCharity = await CharityDataDynamoDB.getObject({
        id: { value: data.charityId, where: "=" },
      });

      if (existingCharity.length === 0) {
        /**
         * Write this to the dynamoDB
         */
        await CharityDataDynamoDB.create(data.id, {
          ...data,
        });
      } else {
        /**
         * Update this charity info
         */
        await CharityDataDynamoDB.updateById(data.id, {
          ...data,
        });
      }
    },
  });

  logger.info(`Finished Parsing IRS Data 🍾`);
}
