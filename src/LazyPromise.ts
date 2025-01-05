export const toLazyPromise = <TPromiseResult, TObject extends object>(
  execute: () => Promise<TPromiseResult>,
  object: TObject,
): Omit<TObject, "then"> & Promise<TPromiseResult> => {
  let promise: Promise<TPromiseResult> | undefined;

  const getPromise = () => {
    return promise || (promise = execute());
  };

  return Object.assign(object, {
    // biome-ignore lint: no-then
    then: (onfufilled?: any, onrejected?: any) => {
      return getPromise().then(onfufilled, onrejected);
    },

    catch: (...args: any[]) => {
      // @ts-ignore
      return getPromise().catch.apply(getPromise(), args);
    },

    finally: (onfinally: any) => {
      return getPromise().finally(onfinally);
    },
  }) as any;
};
