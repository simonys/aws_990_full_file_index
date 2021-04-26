# AWS 990 Full File Index

***
**The current index files uploaded are accurate as of April 19, 2021.**
***

The index files provided by AWS for retrieving public IRS 990 tax forms account for only ~75-85% of files that AWS actually has on hand. This repository provides the same index with the complete listing of files available on AWS as well as the code that created the index files.

Note that the files that AWS has is not necessarily a comprehensive sample of 990s e-filed by nonprofits to the IRS. For recent years, it is the most comprehensive source that I am aware of.

- Index files are provided in [index_files](./index_files/)
- Code for producing the files is in [code](./code/)

As I am a young researcher hoping for a career in academia. If you publish any work using this data please cite me appropriately. The data is also on [Harvard Dataverse](https://doi.org/10.7910/DVN/BYJAPN) as is citation information.

## Credit

Building these index files relied heavily on two major resources:
- David Bruce Borenstein's blog post "[Don't use the IRS 990 e-file indices](https://appliednonprofitresearch.com/posts/2020/06/skip-the-irs-990-efile-indices/)"
- Jacob Fenton's [IRSx code](https://github.com/jsfenfen/990-xml-reader) for retrieving information from AWS 990 files.

## Future Plans:

- Provide index files from 2009-2014
- Clean up code. I wanted to make this accessible as quickly as possible and know there are a lot of improvements to make.
- Create/Alter code to allow for regular updating of index files 

