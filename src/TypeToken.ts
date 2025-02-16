export type TypeToken<T> = {
  _?: never & T;
};

export type TypeOfToken<T> = T extends TypeToken<infer U> ? U : never;

export const typeToken = <T>(): TypeToken<T> => {
  return {} as TypeToken<T>;
};
