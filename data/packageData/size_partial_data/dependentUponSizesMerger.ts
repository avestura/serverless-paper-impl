// deno run --allow-read --allow-write  .\dependentUponSizesMerger.ts

let merged = {}

for(let i = 1; i <= 14; i++) {
    const name = `dependentUpon_sizes_part${i}.json`
    const file = Deno.readTextFileSync(name)
    const json = JSON.parse(file)
    merged = {
        ...merged,
        ...json
    }
}

Deno.writeTextFileSync("dependentUpon_sizes.json", JSON.stringify(merged), {append: false})