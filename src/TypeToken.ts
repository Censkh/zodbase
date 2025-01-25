export type TypeToken<T> = {
  _?: never & T;
};

export type TypeOfToken<T> = T extends TypeToken<infer U> ? U : never;

export const createTypeToken = <T>(): TypeToken<T> => {
  return {} as TypeToken<T>;
};
