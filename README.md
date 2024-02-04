# Form 990 Parser 🧑‍💻

This code is meant to be used as starter code for anyone that interested in parsing IRS data directly. 

We've written a whole blog post on how this came to be which you can check out [here](https://getsparechange.com/blog/technology/building_sparechange). 

I have not had the time yet to format the code nicely, so its quite rough. If there is any interest, I'm happy to clean it up just create an issue!

# Storage

There is a type referenced `CharityDataDynamoDB` which is a custom model we use to store object to our dynamoDB. This can be used with any other database provider, assuming you can expose an interface that works in the code. 

If there are any issues here or anything that can be clarified please open an issue. 