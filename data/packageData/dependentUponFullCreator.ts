// deno run --allow-read --allow-write  .\dependentUponFullCreator.ts

const sizeData = JSON.parse(Deno.readTextFileSync("dependentUpon_sizes.json"))
const depData = JSON.parse(Deno.readTextFileSync("dependentUpon.json"))

const result: Record<string, any> = {}
for(const key in depData) {
    const sizeInfo = sizeData[key]
    const depInfo = depData[key]
    result[key] = {
        dependencyCount: depInfo,
        ...sizeInfo
    }
}

Deno.writeTextFileSync("dependentUpon_full.json", JSON.stringify(result))