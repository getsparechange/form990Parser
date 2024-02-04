import Bluebird from "bluebird";
import fs from "fs";
import path from "path";
import { parse } from "csv-parse";
import CharityDataDynamoDB from "somewhere";
import bunyan from "bunyan";

const logger = bunyan.createLogger({ name: "parseIRSData-MatchNteeCode" });

interface RowDetails {
  EIN: string;
  NAME: string;
  ICO: string;
  STREET: string;
  CITY: string;
  STATE: string;
  ZIP: string;
  GROUP: string;
  SUBSECTION: string;
  AFFILIATION: string;
  CLASSIFICATION: string;
  RULING: string;
  DEDUCTIBILITY: string;
  FOUNDATION: string;
  ACTIVITY: string;
  ORGANIZATION: string;
  STATUS: string;
  TAX_PERIOD: string;
  ASSET_CD: string;
  INCOME_CD: string;
  FILING_REQ_CD: string;
  PF_FILING_REQ_CD: string;
  ACCT_PD: string;
  ASSET_AMT: string;
  INCOME_AMT: string;
  REVENUE_AMT: string;
  NTEE_CD: string;
  SORT_NAME: string;
}
/**
 * Loop through all available
 * @param pathToFolder Path to the folder full of CSV from reference below
 * @param concurrency How many concurrent async process' to spin up
 * @see https://www.irs.gov/charities-non-profits/exempt-organizations-business-master-file-extract-eo-bmf
 */
export default async function matchNTEECode(args: {
  pathToFolder: string;
  concurrency?: number;
}): Promise<void> {
  const { pathToFolder, concurrency = 40 } = args;
  logger.info(`Starting to match NTEE codes... 🏃‍♀️`);

  // Get the files as an array
  const files = await fs.promises.readdir(pathToFolder);

  await Bluebird.map(
    files,
    async (file) => {
      const filePath = path.join(pathToFolder, file);
      try {
        /**
         * Extract and parse the CSV
         */
        const results: any[] = [];
        await new Promise((resolve) => {
          fs.createReadStream(filePath)
            .pipe(parse())
            .on("data", (data) => results.push(data))
            .on("end", () => {
              resolve(null);
            });
        });

        /**
         * The first row is our headers, so ignore that
         */
        const rawData = results.slice(1, results.length);

        let index = 0;
        await Bluebird.map(
          rawData,
          async (row: string[]) => {
            index += 1;
            if (index % 1000 === 0) {
              logger.info(`On index: ${index} for ${file}`);
            }
            try {
              const rowParsed = _rowToTyped(row);

              const ein = rowParsed.EIN;
              const nteeCode = rowParsed.NTEE_CD;
              const activityCode = rowParsed.ACTIVITY;

              /**
               * Now we need to lookup this data in our DB
               */
              const existingCharity = await CharityDataDynamoDB.getObject({
                id: { value: ein, where: "=" },
              });

              if (!existingCharity) {
                logger.error(
                  `Error processing: ${ein} could not find existing charity`
                );
                return;
              }

              const update: { nteeCode?: string; activityCode?: string } = {};
              if (nteeCode === undefined && activityCode === undefined) {
                logger.warn(`No NteeCode or ActivityCode for: ${ein}`);
                return;
              }
              if (nteeCode) {
                update.nteeCode = nteeCode;
              }
              if (activityCode) {
                update.activityCode = activityCode;
              }

              /**
               * Save the data on our charity model
               */
              await CharityDataDynamoDB.updateById(ein, {
                ...update,
              });

              /**
               * Sleep for a bit to avoid rate limiting ourselves
               */
              await new Promise((resolve) => setTimeout(resolve, 500));
            } catch (err) {
              logger.error(`Unhandled error processing row: ${row}`, err);
              return;
            }
          },
          { concurrency }
        );
        logger.info(`Done processing: ${filePath} 🎉`);
      } catch (err) {
        logger.error(`Error with parsing filepath: ${filePath}`, err);
      }
    },
    { concurrency: 1 }
  );

  logger.info(`All done updating NTEE codes! 🍾`);
}

function _rowToTyped(data: string[]): RowDetails {
  return {
    EIN: data[0],
    NAME: data[1],
    ICO: data[2],
    STREET: data[3],
    CITY: data[4],
    STATE: data[5],
    ZIP: data[6],
    GROUP: data[7],
    SUBSECTION: data[8],
    AFFILIATION: data[9],
    CLASSIFICATION: data[10],
    RULING: data[11],
    DEDUCTIBILITY: data[12],
    FOUNDATION: data[13],
    ACTIVITY: data[14],
    ORGANIZATION: data[15],
    STATUS: data[16],
    TAX_PERIOD: data[17],
    ASSET_CD: data[18],
    INCOME_CD: data[19],
    FILING_REQ_CD: data[20],
    PF_FILING_REQ_CD: data[21],
    ACCT_PD: data[22],
    ASSET_AMT: data[23],
    INCOME_AMT: data[24],
    REVENUE_AMT: data[25],
    NTEE_CD: data[26],
    SORT_NAME: data[27],
  };
}
