kixi.ckan
=========

Dev setup:

1. In your home directory create a `.ckan.edn` with the contents below:
   ```edn
   {:ckan-client {:site "<site>/api/3/action/"
                  :api-key "<your_private_key>"}}
    ```

2. Do `M-x cider-jack-in` in kixi.ckan project
3. Run `(go)`
4. Open up `kixi.ckan/dev.dev.clj` and try out some of the function there, e.g.
  - `(get-all-datasets-names system)` will print a list of *all* datasets in this client's CKAN
  - `(get-package-contents system "hscic_dataset_1")` will get metadata of a package with a specified name.
  - `(get-resource-data system "1aa05f43-4921-41c9-bd80-8bca465f1985")` will get first page of a resource with a given id.
