import * as fs from "fs";
import * as sqlite3 from "sqlite3";
import { open } from "sqlite";
import Web3 from "web3";

// Konfigurasi
const CONFIG = {
  CONTRACT_ADDRESS: "0xf4A5603A0B0d4a7BbDC943CcFcDC2a6E75741A0e", // Ganti dengan address contract Anda
  RPC_URLS: [
    "https://bsc-mainnet.public.blastapi.io", // Primary
    "https://bsc-dataseed1.binance.org", // Backup 1
    "https://bsc-dataseed2.binance.org", // Backup 2
    "https://bsc-dataseed3.binance.org", // Backup 3
  ],
  CHAIN_ID: "56", // BNB Smart Chain (BSC)
  START_BLOCK: 58038338, // Block awal untuk mulai scanning (range kecil untuk test)
  END_BLOCK: 73261345, // Block akhir untuk scanning
  DB_PATH: "./registrations.db",
  CSV_PATH: "./registrations.csv",
  BATCH_SIZE: 1000, // Jumlah blocks per batch
  MAX_RETRIES: 5, // Maksimal retry per request
};

// Interface untuk event Registration
interface RegistrationEvent {
  address: string;
  referrer: string;
  blockNumber: number;
  transactionHash: string;
  timestamp?: number;
  logIndex: number;
}

// Fungsi helper untuk retry dengan exponential backoff
async function retryWithBackoff<T>(
  fn: () => Promise<T>,
  maxRetries: number = CONFIG.MAX_RETRIES,
  baseDelay: number = 1000,
): Promise<T> {
  let lastError: any;

  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      return await fn();
    } catch (error: any) {
      lastError = error;

      if (attempt === maxRetries) {
        throw error;
      }

      const delay = baseDelay * Math.pow(2, attempt);
      console.log(
        `   ⚠️  Attempt ${attempt + 1} failed, retrying in ${delay}ms...`,
      );
      await new Promise((resolve) => setTimeout(resolve, delay));
    }
  }

  throw lastError;
}

// Fungsi untuk membuat Web3 instance dengan RPC tertentu
function createWeb3Instance(rpcUrl: string): Web3 {
  return new Web3(rpcUrl);
}

// Fungsi untuk mendapatkan event logs dari BSC node
async function getRegistrationEvents(): Promise<RegistrationEvent[]> {
  let currentRpcIndex = 0;
  let web3 = createWeb3Instance(CONFIG.RPC_URLS[currentRpcIndex]);

  try {
    // Event signature untuk Registration(address indexed _from, address indexed referrer)
    // keccak256("Registration(address,address)")
    const eventTopic =
      "0x625424c0705fbd4a442eed42d551efb612d48d594b1e8c3be0266d553d0a8bc7";

    console.log("Connecting to BSC RPC...");
    console.log(`Chain ID: ${CONFIG.CHAIN_ID} (BNB Smart Chain)`);
    console.log(`Contract: ${CONFIG.CONTRACT_ADDRESS}`);
    console.log(`Using RPC: ${CONFIG.RPC_URLS[currentRpcIndex]}\n`);

    const allEvents: RegistrationEvent[] = [];

    // Get latest block dengan retry
    const latestBlock = await retryWithBackoff(async () => {
      return await web3.eth.getBlockNumber();
    });
    const endBlock = CONFIG.END_BLOCK || Number(latestBlock);
    console.log(`Latest block: ${latestBlock}`);
    console.log(`Scanning from block ${CONFIG.START_BLOCK} to ${endBlock}\n`);

    let currentBlock = CONFIG.START_BLOCK;
    let batchCount = 0;
    const SAVE_INTERVAL = 100; // Save every 10 batches

    while (currentBlock <= endBlock) {
      const toBlock = Math.min(currentBlock + CONFIG.BATCH_SIZE - 1, endBlock);

      console.log(`🔍 Scanning blocks ${currentBlock} to ${toBlock}...`);

      try {
        // Get logs dengan retry - temporarily remove topics filter to see all events
        const logs = await retryWithBackoff(async () => {
          return await web3.eth.getPastLogs({
            address: CONFIG.CONTRACT_ADDRESS,
            // topics: [eventTopic], // Temporarily removed to see all events
            fromBlock: currentBlock,
            toBlock: toBlock,
          });
        });

        console.log(`   Found ${logs.length} events in this batch`);

        // Debug: log all event signatures found
        const signatures = logs
          .map((log) => (log as any).topics?.[0])
          .filter(Boolean);
        if (signatures.length > 0) {
          console.log(
            `   📝 Event signatures in this batch: ${signatures.join(", ")}`,
          );
        }

        // Parse logs menjadi array RegistrationEvent
        const batchEvents: RegistrationEvent[] = [];
        for (const log of logs) {
          try {
            const logData = log as any; // Cast to any to access properties

            // Skip logs that don't have the expected topics structure
            if (!logData.topics || logData.topics.length < 1) {
              console.log(
                `   ⚠️  Skipping log with invalid topics: ${
                  logData.topics?.length || 0
                }`,
              );
              continue; // Skip this log
            }

            // Check if this is our event signature
            if (logData.topics[0] !== eventTopic) {
              console.log(
                `   ⚠️  Skipping log with wrong signature: ${logData.topics[0]}`,
              );
              continue;
            }

            let address: string;
            let referrer: string;

            // For events with indexed params
            if (logData.topics.length >= 3) {
              // topics[0] = event signature
              // topics[1] = indexed _from (address yang register)
              // topics[2] = indexed referrer
              address = "0x" + logData.topics[1].slice(26); // Ambil 40 karakter terakhir
              referrer = "0x" + logData.topics[2].slice(26);
            } else {
              // For events without indexed params, parse from log.data
              // Assuming Registration(address,address) - 64 bytes each
              const data = logData.data || "0x";
              if (data.length < 130) {
                // 0x + 64 + 64 bytes
                console.log(
                  `   ⚠️  Skipping log with insufficient data: ${data.length} chars`,
                );
                continue;
              }

              address = "0x" + data.slice(26, 66); // First 32 bytes (after 0x)
              referrer = "0x" + data.slice(66, 106); // Next 32 bytes
            }

            // Validate addresses
            if (
              !address ||
              !address.startsWith("0x") ||
              address.length !== 42
            ) {
              console.log(`   Invalid address: ${address}`);
              continue;
            }
            // Referrer validation removed - can be zero address

            // Get block timestamp dengan retry
            const block = await retryWithBackoff(async () => {
              return await web3.eth.getBlock(logData.blockNumber);
            });
            const timestamp = Number(block.timestamp);

            const event: RegistrationEvent = {
              address: address.toLowerCase(),
              referrer: referrer.toLowerCase(),
              blockNumber: Number(logData.blockNumber),
              transactionHash: logData.transactionHash,
              timestamp: timestamp,
              logIndex: Number(logData.logIndex),
            };

            batchEvents.push(event);
            allEvents.push(event);
          } catch (parseError) {
            console.log(`   Error parsing log: ${parseError}`);
            continue;
          }
        }

        // Save batch data incrementally
        if (batchEvents.length > 0) {
          const inserted = await saveBatchToSQLite(batchEvents);
          console.log(
            `   💾 Saved ${inserted} events to database (${
              batchEvents.length - inserted
            } duplicates skipped)`,
          );
        }

        currentBlock = toBlock + 1;
        batchCount++;

        // Periodic full save and progress update
        if (batchCount % SAVE_INTERVAL === 0) {
          console.log(`\nProgress Update:`);
          console.log(`   Processed ${batchCount} batches`);
          console.log(`   Current block: ${currentBlock}`);
          console.log(`   Total events so far: ${allEvents.length}`);
          console.log(`   Next save at batch ${batchCount + SAVE_INTERVAL}\n`);
        }

        // Add small delay to avoid rate limiting
        await new Promise((resolve) => setTimeout(resolve, 100));
      } catch (error) {
        console.error(
          `   ❌ Error scanning blocks ${currentBlock}-${toBlock}:`,
          error,
        );

        // Jika retry gagal, coba switch RPC
        currentRpcIndex = (currentRpcIndex + 1) % CONFIG.RPC_URLS.length;
        web3 = createWeb3Instance(CONFIG.RPC_URLS[currentRpcIndex]);
        console.log(
          `   🔄 Switching to RPC: ${CONFIG.RPC_URLS[currentRpcIndex]}`,
        );

        // Tunggu sebelum retry dengan RPC baru
        await new Promise((resolve) => setTimeout(resolve, 2000));
        continue;
      }
    }

    console.log(
      `\n✅ Scanning complete! Total events found: ${allEvents.length}\n`,
    );
    return allEvents;
  } catch (error: any) {
    console.error("❌ Error connecting to BSC RPC:", error.message);
    throw error;
  }
}

// Fungsi untuk menyimpan batch data ke SQLite (incremental)
async function saveBatchToSQLite(events: RegistrationEvent[]): Promise<number> {
  const db = await open({
    filename: CONFIG.DB_PATH,
    driver: sqlite3.Database,
  });

  // Buat tabel jika belum ada
  await db.exec(`
    CREATE TABLE IF NOT EXISTS registrations (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      address TEXT NOT NULL,
      referrer TEXT NOT NULL,
      block_number INTEGER NOT NULL,
      transaction_hash TEXT NOT NULL,
      log_index INTEGER NOT NULL,
      timestamp INTEGER,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      UNIQUE(transaction_hash, log_index)
    )
  `);

  // Buat index jika belum ada
  await db.exec(`
    CREATE INDEX IF NOT EXISTS idx_address ON registrations(address);
    CREATE INDEX IF NOT EXISTS idx_referrer ON registrations(referrer);
    CREATE INDEX IF NOT EXISTS idx_block_number ON registrations(block_number);
    CREATE INDEX IF NOT EXISTS idx_timestamp ON registrations(timestamp);
  `);

  // Insert data menggunakan prepared statement
  const stmt = await db.prepare(`
    INSERT OR IGNORE INTO registrations
    (address, referrer, block_number, transaction_hash, log_index, timestamp)
    VALUES (?, ?, ?, ?, ?, ?)
  `);

  let inserted = 0;
  for (const event of events) {
    const result = await stmt.run(
      event.address,
      event.referrer,
      event.blockNumber,
      event.transactionHash,
      event.logIndex,
      event.timestamp,
    );
    if (result.changes && result.changes > 0) inserted++;
  }

  await stmt.finalize();
  await db.close();

  // Return inserted count for logging
  return inserted;
}

// Fungsi untuk menyimpan ke CSV
function saveToCSV(events: RegistrationEvent[]): void {
  console.log("Saving to CSV...");

  // Header CSV
  const headers =
    "Address,Referrer,Block Number,Transaction Hash,Timestamp,Date\n";

  // Convert events ke CSV rows
  const rows = events
    .map((event) => {
      const date = event.timestamp
        ? new Date(event.timestamp * 1000).toISOString()
        : "";
      return `${event.address},${event.referrer},${event.blockNumber},${
        event.transactionHash
      },${event.timestamp || ""},${date}`;
    })
    .join("\n");

  // Tulis ke file
  fs.writeFileSync(CONFIG.CSV_PATH, headers + rows, "utf-8");

  console.log(`✓ ${events.length} registrations saved to: ${CONFIG.CSV_PATH}`);
}

// Fungsi untuk menampilkan statistik
async function displayStatistics(events: RegistrationEvent[]): Promise<void> {
  console.log("\n" + "=".repeat(60));
  console.log("REGISTRATION STATISTICS");
  console.log("=".repeat(60));

  console.log(`\nOverview:`);
  console.log(`   Total Registrations: ${events.length}`);

  // Hitung unique addresses
  const uniqueAddresses = new Set(events.map((e) => e.address));
  console.log(`   Unique Addresses: ${uniqueAddresses.size}`);

  // Hitung unique referrers (exclude zero address)
  const uniqueReferrers = new Set(
    events
      .map((e) => e.referrer)
      .filter((r) => r !== "0x0000000000000000000000000000000000000000"),
  );
  console.log(`   Unique Referrers: ${uniqueReferrers.size}`);

  // Block range
  if (events.length > 0) {
    const blocks = events.map((e) => e.blockNumber).sort((a, b) => a - b);
    console.log(`\nBlock Range:`);
    console.log(`   First Registration: Block ${blocks[0]}`);
    console.log(`   Latest Registration: Block ${blocks[blocks.length - 1]}`);
  }

  // Time range
  const timestamps = events
    .map((e) => e.timestamp)
    .filter((t): t is number => t !== undefined)
    .sort((a, b) => a - b);

  if (timestamps.length > 0) {
    const firstDate = new Date(timestamps[0] * 1000);
    const lastDate = new Date(timestamps[timestamps.length - 1] * 1000);
    console.log(`\nTime Range:`);
    console.log(`   First: ${firstDate.toLocaleString()}`);
    console.log(`   Latest: ${lastDate.toLocaleString()}`);
  }

  // Calculate referrer counts
  const referrerCounts = events.reduce(
    (acc: { [key: string]: number }, event) => {
      if (event.referrer !== "0x0000000000000000000000000000000000000000") {
        acc[event.referrer] = (acc[event.referrer] || 0) + 1;
      }
      return acc;
    },
    {},
  );

  const topReferrers = Object.entries(referrerCounts)
    .sort(([, a], [, b]) => b - a)
    .slice(0, 10);

  console.log(`\nTop 10 Referrers:`);
  console.log("-".repeat(60));
  topReferrers.forEach(([address, count], index) => {
    const percentage = ((count / events.length) * 100).toFixed(2);
    console.log(`${(index + 1).toString().padStart(2)}. ${address}`);
    console.log(`    └─ ${count} referrals (${percentage}%)`);
  });

  // Registrations without referrer (root accounts)
  const rootAccounts = events.filter(
    (e) => e.referrer === "0x0000000000000000000000000000000000000000",
  );
  console.log(`\nRoot Accounts (No Referrer): ${rootAccounts.length}`);

  console.log("\n" + "=".repeat(60) + "\n");
}

// Fungsi untuk generate final reports (CSV dan summary)
async function generateFinalReports(
  events: RegistrationEvent[],
): Promise<void> {
  console.log("\nGenerating final reports...");

  // Generate CSV
  saveToCSV(events);

  // Generate summary JSON
  exportSummaryJSON(events);

  console.log("Final reports generated");
}

// Fungsi untuk export summary JSON
function exportSummaryJSON(events: RegistrationEvent[]): void {
  const referrerCounts = events.reduce(
    (acc: { [key: string]: number }, event) => {
      if (event.referrer !== "0x0000000000000000000000000000000000000000") {
        acc[event.referrer] = (acc[event.referrer] || 0) + 1;
      }
      return acc;
    },
    {},
  );

  const summary = {
    totalRegistrations: events.length,
    uniqueAddresses: new Set(events.map((e) => e.address)).size,
    uniqueReferrers: new Set(
      events
        .map((e) => e.referrer)
        .filter((r) => r !== "0x0000000000000000000000000000000000000000"),
    ).size,
    topReferrers: Object.entries(referrerCounts)
      .sort(([, a], [, b]) => b - a)
      .slice(0, 20)
      .map(([address, count]) => ({ address, count })),
    timestamp: new Date().toISOString(),
  };

  fs.writeFileSync(
    "./registrations_summary.json",
    JSON.stringify(summary, null, 2),
    "utf-8",
  );

  console.log("Summary exported to: registrations_summary.json");
}

// Main function
async function main() {
  try {
    console.log("BSC Registration Scraper (Direct RPC)\n");

    // Validasi konfigurasi
    if (
      !CONFIG.CONTRACT_ADDRESS ||
      CONFIG.CONTRACT_ADDRESS === "0xYourContractAddress"
    ) {
      throw new Error("Please set CONTRACT_ADDRESS in CONFIG");
    }

    // Ambil event dari BSC node (data sudah tersimpan incremental)
    const events = await getRegistrationEvents();

    if (events.length === 0) {
      console.log("No registration events found.");
      console.log("    Make sure:");
      console.log("    1. Contract address is correct");
      console.log("    2. Contract has Registration events");
      console.log("    3. START_BLOCK is correct");
      return;
    }

    // Generate final reports (CSV and summary)
    await generateFinalReports(events);

    // Tampilkan statistik
    await displayStatistics(events);

    console.log("Done! Check the following files:");
    console.log(
      `   - ${CONFIG.DB_PATH} (SQLite database - updated incrementally)`,
    );
    console.log(`   - ${CONFIG.CSV_PATH} (CSV export)`);
    console.log(`   - registrations_summary.json (Summary)`);
  } catch (error: any) {
    console.error("\n❌ Error:", error.message);
    console.error("\nTroubleshooting:");
    console.error("1. Check if BSC RPC endpoint is accessible");
    console.error("2. Verify contract address is correct");
    console.error("3. Make sure you have internet connection");
    console.error(
      "4. Check if START_BLOCK and END_BLOCK are correct (contract deployment block range)",
    );
    console.error(
      "5. If rate limited, increase delay between requests or reduce BATCH_SIZE",
    );
    process.exit(1);
  }
}

// Jalankan script
main();
