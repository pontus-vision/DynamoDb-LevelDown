declare module 'level-supports' {
  type SupportManifest = {
    // Features of abstract-leveldown
    bufferKeys?: boolean;
    snapshots?: boolean;
    permanence?: boolean;
    seek?: boolean;
    clear?: boolean;

    // Features of abstract-leveldown that levelup doesn't have
    status?: boolean;

    // Features of disk-based implementations
    createIfMissing?: boolean;
    errorIfExists?: boolean;

    // Features of level(up) that abstract-leveldown doesn't have yet
    deferredOpen?: boolean;
    openCallback?: boolean;
    promises?: boolean;
    streams?: boolean;
    encodings?: boolean;

    // Methods that are not part of abstract-leveldown or levelup
    additionalMethods?: { [key: string]: boolean };
  };

  function supports(manifest: SupportManifest): SupportManifest;

  export { SupportManifest };
  export default supports;
}
