const fs = require('fs');
const path = require('path');

// Create public directory if it doesn't exist
if (!fs.existsSync(path.join(__dirname, 'public'))) {
  fs.mkdirSync(path.join(__dirname, 'public'), { recursive: true });
}

// Copy all HTML files from backend/public to public
const htmlFiles = fs.readdirSync(path.join(__dirname, 'backend/public')).filter(file => file.endsWith('.html'));
htmlFiles.forEach(file => {
  const sourcePath = path.join(__dirname, 'backend/public', file);
  const destPath = path.join(__dirname, 'public', file);
  
  let content = fs.readFileSync(sourcePath, 'utf8');
  
  // Replace API_BASE_URL placeholder in dashboard.html
  if (file === 'dashboard.html') {
    const apiUrl = process.env.API_BASE_URL || 'http://localhost:3000';
    content = content.replace(/%%API_BASE_URL%%/g, apiUrl);
    console.log('✅ Dashboard built with API URL:', apiUrl);
  }
  
  fs.writeFileSync(destPath, content);
  console.log('✅ Copied:', file);
});

console.log('🎉 Frontend build completed!');
