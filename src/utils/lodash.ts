export function noop() {}

export function delay(ms: number) {
  return new Promise((resolve, reject) => setTimeout(resolve, ms));
}
