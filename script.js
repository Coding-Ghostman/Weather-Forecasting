const app = document.querySelector(".weather-app");
const temp = document.querySelector(".temp");
const dataOutput = document.querySelector(".data");
const timeOutput = document.querySelector(".time");
const conditionOutput = document.querySelector(".condition");
const nameOutput = document.querySelector(".name");
const icon = document.querySelector(".icon");
const cloudOutput = document.querySelector(".cloud");
const humidityOutput = document.querySelector(".humidity");
const windOutput = document.querySelector(".wind");
const form = document.querySelector(".locationInput");
const search = document.querySelector(".search");
const btn = document.querySelector(".submit");
const cities = document.querySelector(".cities");

// Default city when the page loads.
let cityInput = "London";

// Add click event to each city in the panel\
cities.forEach((city) => {
   city.addEventListener("click", (e) => {
      cityInput = e.target.innerHTML;
      fetchWeatherData();
      app.style.opacity = "0";
   });
});

// Add submit event to the form
form.addEventListener("submit", (e) => {
   if (search.value.length == 0) {
      alert("Please type in a city name");
   } else {
      cityInput = search.value;
      fetchWeatherData();
      search.value = "";
      app.style.oppacity = "0";
   }

   e.preventDefault();
});

function dayOfTheWeek(day, month, year) {
   const weekday = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"];
   return weekday[new Data("${day}/${month}/${year}").getDay()];
}

function fetchWeatherData() {}
