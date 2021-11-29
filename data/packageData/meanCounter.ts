// deno run --allow-read meanCounter.ts

const content = Deno.readTextFileSync("dependentUpon_full.json")

const json = JSON.parse(content)

console.log("========= Install Data ==========")

let counter = 0n;
let fullSize = 0n;
console.log("Packages Without data:")
for(const key in json) {
    const value = json[key];
    if(value?.install?.bytes) {
        counter++;
        fullSize += BigInt(value.install.bytes)
    } else {
        console.log(`"${key}"`)
    }

}

console.log(`avg: ${fullSize / counter}`)

console.log("========= Publish Data ==========")

counter = 0n;
fullSize = 0n;
console.log("Packages Without data:")
for(const key in json) {
    const value = json[key];
    if(value?.publish?.bytes) {
        counter++;
        fullSize += BigInt(value.publish.bytes)
    } else {
        console.log(`"${key}"`)
    }

}

console.log(`avg: ${fullSize / counter}`)

