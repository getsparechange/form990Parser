import bunyan from "bunyan";
import fs from "fs";
import CharityDataDynamoDB from "somewhere";
import xml2JS from "xml2js";
import path from "path";
import Bluebird from "bluebird";
import { CharityDataDynamo } from "somewhere";
import { deflate } from "zlib";

const logger = bunyan.createLogger({
  name: "parseIRSData-loopOverAllCharities",
});

const loopOverAllCharities = async (args: {
  pathToFolder: string;
  callback: (input: Omit<CharityDataDynamo, "createdAt">) => Promise<void>;
  dynamoEnabled?: boolean;
  concurrency?: number;
}) => {
  logger.info(`Starting to loop through all charities... 🦑`);
  const { pathToFolder, callback, dynamoEnabled = true, concurrency } = args;

  // Get the files as an array
  const files = await fs.promises.readdir(pathToFolder);
  for (const file of files) {
    if (file.endsWith(".zip") || file === ".DS_Store") {
      logger.info(
        `Ignoring file: ${file} since it looks like a zip/ds_store, and we need extracted files 👽`
      );
      continue;
    }

    /**
     * Build a path to the file, and then loop through it and save to our database
     */
    const folderPath = path.join(pathToFolder, file);
    logger.info(`Looping through: ${folderPath}....`);

    await loopThroughFolder({
      folderPath,
      folderName: file,
      callback,
      dynamoEnabled,
      concurrency,
    });
  }
  logger.info(`Finished looping through all charities 🍾`);
};

async function loopThroughFolder(args: {
  folderPath: string;
  folderName: string;
  dynamoEnabled: boolean;
  concurrency?: number;
  callback: (input: Omit<CharityDataDynamo, "createdAt">) => Promise<void>;
}): Promise<void> {
  const {
    folderPath,
    folderName,
    concurrency = 40,
    callback,
    dynamoEnabled,
  } = args;

  /**
   * List of files that we fail to parse
   */
  const badFiles: string[] = [];

  // Get the files as an array
  const files = await fs.promises.readdir(folderPath);

  let index = 0;
  await Bluebird.map(
    files,
    async (file) => {
      const filePath = `${folderPath}/${file}`;
      if (index % 1000 === 0) {
        logger.info(`On index: ${index} 📈`);
      }
      index += 1;

      /**
       * If we are using dynamo in our call back, sleep every so often for dynamo
       */
      if (dynamoEnabled) {
        if (index % 500 === 0) {
          logger.info(`On index: ${index}. Sleeping for 10 sec 💤`);
          await new Promise((resolve) => setTimeout(resolve, 10 * 1000));
        }
      }

      try {
        /**
         * Parse the XML file
         */
        const dataRaw = fs.readFileSync(filePath);

        const parsedJSON = await new Promise((resolve, reject) =>
          xml2JS.parseString(dataRaw, (err, data) => {
            if (err) {
              reject(err);
            } else {
              resolve(data);
            }
          })
        );
        /**
         * Extract the eni, we'll use that as charityId
         */
        // @ts-ignore
        const ein = parsedJSON.Return.ReturnHeader[0].Filer[0].EIN[0];

        let charityName: undefined;
        try {
          charityName =
            // @ts-ignore
            parsedJSON.Return.ReturnHeader[0].Filer[0].BusinessName[0]
              .BusinessNameLine1Txt[0];
        } catch (err) {
          /**
           * We at least need this
           */
          throw new Error(`Unable to parse charityName`);
        }

        let address_line_1: undefined;
        let city: undefined;
        let state: undefined | string;
        let zip: undefined | string;
        let country: undefined | string;
        try {
          const usAddress =
            // @ts-ignore
            parsedJSON.Return.ReturnHeader[0].Filer[0].USAddress[0];

          address_line_1 = usAddress.AddressLine1Txt[0];
          city = usAddress.CityNm[0];
          state = usAddress.StateAbbreviationCd[0];
          zip = usAddress.ZIPCd[0];
          country = "United States";
        } catch (err) {}

        let websiteURL: undefined;
        let mission: undefined;
        let ruling_year: undefined | number;
        let number_of_volunteers: undefined | number;
        let number_of_employees: undefined | number;
        let number_of_employees_over_50k: undefined | number;
        let total_revenue: undefined | number;
        let total_expenses: undefined | number;
        let expense_ratio: undefined | number;
        let total_assets: undefined | number;
        let status501: boolean;

        // @ts-ignore
        const formType = Object.keys(parsedJSON.Return.ReturnData[0])[1];
        switch (formType) {
          case "IRS990": {
            try {
              status501 =
                // @ts-ignore
                parsedJSON.Return.ReturnData[0].IRS990[0]
                  .Organization501c3Ind[0] === "X";
            } catch (err) {
              status501 = false;
            }

            try {
              websiteURL =
                // @ts-ignore
                parsedJSON.Return.ReturnData[0].IRS990[0].WebsiteAddressTxt[0];
            } catch (err) {}

            try {
              mission =
                // @ts-ignore
                parsedJSON.Return.ReturnData[0].IRS990[0]
                  .ActivityOrMissionDesc[0];
            } catch (err) {}

            try {
              ruling_year = Number(
                // @ts-ignore
                parsedJSON.Return.ReturnData[0].IRS990[0].FormationYr[0]
              );
            } catch (err) {}

            try {
              number_of_volunteers = Number(
                // @ts-ignore
                parsedJSON.Return.ReturnData[0].IRS990[0].TotalVolunteersCnt[0]
              );
            } catch (err) {}

            try {
              number_of_employees = Number(
                // @ts-ignore
                parsedJSON.Return.ReturnData[0].IRS990[0].TotalEmployeeCnt[0]
              );
            } catch (err) {}

            try {
              total_revenue = Number(
                // @ts-ignore
                parsedJSON.Return.ReturnData[0].IRS990[0].RevenueAmt[0]
              );
            } catch (err) {
              try {
                /**
                 * Try to pull from elsewhere
                 */
                total_revenue = Number(
                  // @ts-ignore
                  parsedJSON.Return.ReturnData[0].IRS990[0].TotalRevenueGrp[0]
                    .TotalRevenueColumnAmt[0]
                );
              } catch (err) {}
            }

            try {
              total_expenses = Number(
                // @ts-ignore
                parsedJSON.Return.ReturnData[0].IRS990[0].ExpenseAmt[0]
              );
            } catch (err) {}

            try {
              total_assets = Number(
                // @ts-ignore
                parsedJSON.Return.ReturnData[0].IRS990[0].TotalAssetsEOYAmt[0]
              );
            } catch (err) {}

            if (total_expenses && total_revenue) {
              try {
                expense_ratio = total_revenue / total_expenses;

                if (
                  expense_ratio === null ||
                  (expense_ratio !== 0 && !expense_ratio) ||
                  expense_ratio === undefined
                ) {
                  expense_ratio = 0;
                }
                if (expense_ratio === Number.POSITIVE_INFINITY) {
                  expense_ratio = 1;
                }
              } catch (err) {}
            }

            break;
          }
          case "IRS990PF": {
            try {
              websiteURL =
                // @ts-ignore
                parsedJSON.Return.ReturnData[0].IRS990PF[0]
                  .WebsiteAddressTxt[0];
            } catch (err) {}

            try {
              number_of_employees_over_50k = Number(
                // @ts-ignore
                parsedJSON.Return.ReturnData[0].IRS990PF[0]
                  .OfficerDirTrstKeyEmplInfoGrp[0]
                  .OtherEmployeePaidOver50kCnt[0]
              );
            } catch (err) {}

            try {
              status501 =
                // @ts-ignore
                parsedJSON.Return.ReturnData[0].IRS990PF[0]
                  .Organization501c3Ind[0] === "X";
            } catch (err) {
              status501 = false;
            }

            try {
              total_assets = Number(
                Number(
                  // @ts-ignore
                  parsedJSON.Return.ReturnData[0].IRS990PF[0]
                    .Form990PFBalanceSheetsGrp[0].TotalAssetsEOYAmt
                )
              );
            } catch (err) {}

            if (total_expenses && total_revenue) {
              try {
                expense_ratio = total_revenue / total_expenses;

                if (
                  expense_ratio === null ||
                  (expense_ratio !== 0 && !expense_ratio) ||
                  expense_ratio === undefined
                ) {
                  expense_ratio = 0;
                }
                if (expense_ratio === Number.POSITIVE_INFINITY) {
                  expense_ratio = 1;
                }
              } catch (err) {}
            }

            break;
          }
          case "IRS990EZ": {
            try {
              status501 =
                // @ts-ignore
                parsedJSON.Return.ReturnData[0].IRS990EZ[0]
                  .Organization501c3Ind[0] === "X";
            } catch (err) {
              status501 = false;
            }

            try {
              mission =
                // @ts-ignore
                parsedJSON.Return.ReturnData[0].IRS990EZ[0]
                  .PrimaryExemptPurposeTxt[0];
            } catch (err) {}

            try {
              websiteURL =
                // @ts-ignore
                parsedJSON.Return.ReturnData[0].IRS990EZ[0]
                  .WebsiteAddressTxt[0];
            } catch (err) {}

            try {
              total_revenue = Number(
                // @ts-ignore
                parsedJSON.Return.ReturnData[0].IRS990EZ[0].TotalRevenueAmt[0]
              );
            } catch (err) {}

            try {
              total_expenses = Number(
                // @ts-ignore
                parsedJSON.Return.ReturnData[0].IRS990EZ[0].TotalExpensesAmt[0]
              );
            } catch (err) {}

            try {
              total_assets = Number(
                // @ts-ignore
                parsedJSON.Return.ReturnData[0].IRS990EZ[0]
                  .NetAssetsOrFundBalancesGrp[0].BOYAmt[0]
              );
            } catch (err) {}

            break;
          }
          case "IRS990T": {
            try {
              status501 =
                // @ts-ignore
                parsedJSON.Return.ReturnData[0].IRS990T[0]
                  .Organization501IndicatorGrp[0].Organization501Ind[0] === "X";
            } catch (err) {
              status501 = false;
            }

            break;
          }
          default: {
            logger.error(`Unable to parse: ${formType} form 🚨`);
            throw new Error(`PARSE_ERROR`);
          }
        }

        const input: Omit<CharityDataDynamo, "createdAt"> = {
          charityId: ein,
          status501,
          form990Type: formType as
            | "IRS990"
            | "IRS990PF"
            | "IRS990EZ"
            | "IRS990T",
          websiteURL,
          charityName,
          mission,
          address_line_1,
          city,
          locationState: state,
          zip,
          country,
          total_revenue: Math.abs(total_revenue),
          total_expenses: Math.abs(total_expenses),
          total_assets: Math.abs(total_assets),
          number_of_employees,
          number_of_volunteers,
          ruling_year,
          expense_ratio,
          id: ein,
        };

        if (ein === "620646012") {
          console.log(`>>>>>>>>`, filePath);
        }

        await callback(input);

        /**
         * Sleep in between to avoid rate limits. Can be adjusted as you
         * adjust your read/write units.
         */
        await new Promise((resolve) => setTimeout(resolve, 500));
      } catch (err) {
        logger.error(`Error processing: ${filePath}`, err);
        badFiles.push(filePath);
      }
    },
    { concurrency }
  );

  logger.info(
    `All done parsing: ${folderPath}. Have: ${badFiles.length} errored charities`
  );
  fs.writeFileSync(
    `error-${folderName}.json`,
    JSON.stringify(badFiles, null, 2)
  );
}

export default loopOverAllCharities;
