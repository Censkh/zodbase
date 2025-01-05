export type TypeToken<T> = { __type?: T & never };

export const typeToken = <T>() => ({}) as TypeToken<T>;
