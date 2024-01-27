// var tags_list = "one,two,three";

// tags_list=tags_list.split(",");
// var tags_dict = {};
// // tags_list.dict

// tags_list.forEach(function(value,index) {
//     tags_dict['tag'+(index+1)] = value;
// });
// console.log(tags_dict);

// var tags_list2 = "fine,wine,time";

// tags_list2=tags_list2.split(",");
// console.log(tags_list2);
// tags_list2.forEach(function(value,index) {
//     tags_dict['tag'+(index+8)] = value;
// });
// console.log(tags_dict);


tags = {
    '0':'05119281',
    '1':'6.1.8',
    '2':'Parham-ON'
}
console.log(tags);
var tags_dict = {};

// for (let index in tags) {
//     tags_dict['tag'+(Number(index)+1)] = tags[index];
//     // console.log(index + ' is ' + tags[index])
// }

for (const key in tags) {
    console.log("Key: " + key + ", Value: " + tags[key]);
    tags_dict['tag'+(Number(key)+1)] = tags[key];
  }
console.log(tags_dict);